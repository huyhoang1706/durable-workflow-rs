use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, ItemFn, Meta, Type, spanned::Spanned};

use crate::{
    args::{self, duration_expr, lit_str, lit_u32},
    util::{self, dep_inner, pascal, result_ok, typed},
};

struct Args {
    name: String,
    krate: TokenStream,
    options: Vec<TokenStream>,
}

fn parse_args(attr: TokenStream) -> syn::Result<Args> {
    let mut name = None;
    let mut krate = util::default_crate();
    let mut options = vec![];
    for meta in args::parse_metas(attr)? {
        let key = meta
            .path()
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        match (key.as_str(), &meta) {
            ("name", Meta::NameValue(nv)) => name = Some(lit_str(&nv.value)?),
            ("crate", Meta::NameValue(nv)) => {
                krate = lit_str(&nv.value)?
                    .parse()
                    .map_err(|e| syn::Error::new(nv.span(), format!("{e}")))?;
            }
            ("timeout", Meta::NameValue(nv)) => {
                let d = duration_expr(&nv.value)?;
                options.push(quote! { .timeout(#d) });
            }
            ("concurrency", m) => {
                let (mut limit, mut key) = (None, None);
                for (k, v) in args::pairs(m)? {
                    match k.as_str() {
                        "limit" => limit = Some(lit_u32(&v)?),
                        "key" => key = Some(lit_str(&v)?),
                        other => {
                            return Err(syn::Error::new(
                                v.span(),
                                format!("unknown concurrency option `{other}`"),
                            ));
                        }
                    }
                }
                let limit = limit
                    .ok_or_else(|| syn::Error::new(m.span(), "concurrency needs `limit = N`"))?;
                options.push(match key {
                    Some(k) => quote! { .concurrency_by(#limit, #k) },
                    None => quote! { .concurrency(#limit) },
                });
            }
            ("throttle" | "rate_limit", m) => {
                let (mut limit, mut period, mut burst) = (None, None, None);
                for (k, v) in args::pairs(m)? {
                    match k.as_str() {
                        "limit" => limit = Some(lit_u32(&v)?),
                        "period" => period = Some(duration_expr(&v)?),
                        "burst" if key == "throttle" => burst = Some(lit_u32(&v)?),
                        other => {
                            return Err(syn::Error::new(
                                v.span(),
                                format!("unknown {key} option `{other}`"),
                            ));
                        }
                    }
                }
                let limit = limit
                    .ok_or_else(|| syn::Error::new(m.span(), format!("{key} needs `limit = N`")))?;
                let period = period.ok_or_else(|| {
                    syn::Error::new(m.span(), format!("{key} needs `period = \"1m\"`"))
                })?;
                let method = format_ident!("{}", key);
                options.push(quote! { .#method(#limit, #period) });
                if let Some(b) = burst {
                    options.push(quote! { .throttle_burst(#b) });
                }
            }
            _ => {
                return Err(syn::Error::new(
                    meta.span(),
                    format!("unknown workflow option `{key}`"),
                ));
            }
        }
    }
    let name = name.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[workflow] needs `name = \"...\"`",
        )
    })?;
    Ok(Args {
        name,
        krate,
        options,
    })
}

enum Arg {
    Ctx,
    Dep(Box<Type>),
    Input,
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(attr)?;
    let f: ItemFn = syn::parse2(item)?;
    let is_async = f.sig.asyncness.is_some();
    let krate = &args.krate;

    let mut params = f.sig.inputs.iter();
    let ctx_param = params.next().ok_or_else(|| {
        syn::Error::new(
            f.sig.span(),
            "the first argument must be the workflow context",
        )
    })?;
    typed(ctx_param)?;

    let (mut kinds, mut dep_types, mut input_ty) = (vec![Arg::Ctx], vec![], None::<Type>);
    for p in params {
        let (pt, _) = typed(p)?;
        if let Some(inner) = dep_inner(&pt.ty) {
            kinds.push(Arg::Dep(Box::new(inner.clone())));
            dep_types.push(inner.clone());
        } else if input_ty.is_none() {
            input_ty = Some((*pt.ty).clone());
            kinds.push(Arg::Input);
        } else {
            return Err(syn::Error::new(
                pt.span(),
                "a workflow takes one input argument; the rest must be `Dep<T>`",
            ));
        }
    }
    let input_ty = input_ty.unwrap_or_else(|| syn::parse_quote!(()));
    let has_input = kinds.iter().any(|k| matches!(k, Arg::Input));
    let output_ty = result_ok(&f.sig.output)?;

    let vis = &f.vis;
    let fn_name = &f.sig.ident;
    let struct_name = Ident::new(&pascal(&fn_name.to_string()), fn_name.span());
    let inner_name = format_ident!("__durable_{}", fn_name);
    let mut inner = f.clone();
    inner.sig.ident = inner_name.clone();
    inner.vis = syn::Visibility::Inherited;
    inner.attrs.retain(|a| !a.path().is_ident("doc"));

    // Resolve every `Dep<T>` from the context up front, in declaration order.
    let mut dep_idx = 0usize;
    let mut resolve = vec![];
    let mut call_args = vec![];
    for kind in &kinds {
        match kind {
            Arg::Ctx => call_args.push(quote!(ctx)),
            Arg::Dep(ty) => {
                let v = format_ident!("__dep{dep_idx}");
                dep_idx += 1;
                resolve.push(quote! { let #v = ctx.dep::<#ty>()?; });
                call_args.push(quote!(#v));
            }
            Arg::Input => call_args.push(quote!(input)),
        }
    }
    let input_pat = if has_input {
        quote!(input)
    } else {
        quote!(_input)
    };
    let options = &args.options;
    let name = &args.name;
    let dep_keys = dep_types
        .iter()
        .map(|t| quote! { #krate::DepKey::of::<#t>() });
    let doc_attrs = f.attrs.iter().filter(|a| a.path().is_ident("doc"));

    let run = if is_async {
        quote! {
            async fn run(
                self: ::std::sync::Arc<Self>,
                ctx: #krate::WfAsyncCtx,
                #input_pat: #input_ty,
            ) -> ::core::result::Result<#output_ty, #krate::WorkflowError> {
                #(#resolve)*
                #inner_name(#(#call_args),*).await
            }
        }
    } else {
        quote! {
            fn run(
                self: ::std::sync::Arc<Self>,
                ctx: &#krate::WfSyncCtx,
                #input_pat: #input_ty,
            ) -> ::core::result::Result<#output_ty, #krate::WorkflowError> {
                #(#resolve)*
                #inner_name(#(#call_args),*)
            }
        }
    };
    let trait_name = if is_async {
        quote!(AsyncWorkflow)
    } else {
        quote!(SyncWorkflow)
    };

    Ok(quote! {
        #[allow(non_snake_case, clippy::too_many_arguments)]
        #inner

        #(#doc_attrs)*
        #[derive(Debug, Default, Clone, Copy)]
        #vis struct #struct_name;

        impl #krate::#trait_name for #struct_name {
            const NAME: &'static str = #name;
            type Input = #input_ty;
            type Output = #output_ty;

            fn options() -> #krate::WorkflowOptions {
                #krate::WorkflowOptions::default() #(#options)*
            }

            fn required_deps() -> ::std::vec::Vec<#krate::DepKey> {
                ::std::vec![#(#dep_keys),*]
            }

            #run
        }
    })
}
