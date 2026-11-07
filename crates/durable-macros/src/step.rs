use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, Meta, spanned::Spanned};

use crate::{
    args::{self, duration_expr, lit_str, lit_u32},
    util::{self, dep_inner, result_ok, typed},
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut krate = util::default_crate();
    let (mut retry, mut timeout) = (None, None);
    for meta in args::parse_metas(attr)? {
        let key = meta
            .path()
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        let Meta::NameValue(nv) = &meta else {
            return Err(syn::Error::new(meta.span(), "expected `key = value`"));
        };
        match key.as_str() {
            "retry" => retry = Some(lit_u32(&nv.value)?),
            "timeout" => timeout = Some(duration_expr(&nv.value)?),
            "crate" => {
                krate = lit_str(&nv.value)?
                    .parse()
                    .map_err(|e| syn::Error::new(nv.span(), format!("{e}")))?;
            }
            other => {
                return Err(syn::Error::new(
                    meta.span(),
                    format!("unknown step option `{other}`"),
                ));
            }
        }
    }

    let f: ItemFn = syn::parse2(item)?;
    let is_async = f.sig.asyncness.is_some();
    let ret = f.sig.output.clone();
    let ok_ty = result_ok(&ret)?;
    let ret_ty = match &ret {
        syn::ReturnType::Type(_, t) => t.clone(),
        syn::ReturnType::Default => unreachable!("checked by result_ok"),
    };

    let (mut plain, mut resolve) = (vec![], vec![]);
    for p in &f.sig.inputs {
        let (pt, ident) = typed(p)?;
        match dep_inner(&pt.ty) {
            Some(inner) => resolve.push(quote! { let #ident = ctx.dep::<#inner>()?; }),
            None => plain.push(pt.clone()),
        }
    }

    let (vis, name, attrs, block) = (&f.vis, &f.sig.ident, &f.attrs, &f.block);
    let step_name = name.to_string();
    let policy = retry.map(|n| quote! { #krate::RetryPolicy::exponential(#n) });
    let retry_call = policy.clone().map(|p| quote! { .retry(#p) });
    let timeout_call = timeout.clone().map(|t| quote! { .timeout(#t) });

    let out = if is_async {
        quote! {
            #(#attrs)*
            #vis async fn #name(
                ctx: &#krate::WfAsyncCtx,
                #(#plain),*
            ) -> ::core::result::Result<#ok_ty, #krate::WorkflowError> {
                #(#resolve)*
                ctx.step(#step_name, async move {
                    let __out: #ret_ty = (async move #block).await;
                    __out
                })
                #retry_call
                #timeout_call
                .await
            }
        }
    } else {
        let policy = policy.unwrap_or_else(|| quote! { #krate::RetryPolicy::default() });
        let timeout = match timeout {
            Some(t) => quote! { ::core::option::Option::Some(#t) },
            None => quote! { ::core::option::Option::None },
        };
        let closure = format_ident!("__step_body");
        quote! {
            #(#attrs)*
            #vis fn #name(
                ctx: &#krate::WfSyncCtx,
                #(#plain),*
            ) -> ::core::result::Result<#ok_ty, #krate::WorkflowError> {
                #(#resolve)*
                let #closure = move || {
                    let __out: #ret_ty = (move || #block)();
                    __out
                };
                ctx.step_blocking_with(#step_name, #policy, #timeout, #closure)
            }
        }
    };
    Ok(out)
}
