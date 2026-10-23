use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    FnArg, GenericArgument, Ident, Pat, PatType, PathArguments, ReturnType, Type, spanned::Spanned,
};

/// `Dep<T>` (any path ending in `Dep`) to `T`.
pub fn dep_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(p) = ty else { return None };
    let seg = p.path.segments.last()?;
    if seg.ident != "Dep" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    match args.args.first()? {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    }
}

/// `Result<T, E>` to `T`.
pub fn result_ok(output: &ReturnType) -> syn::Result<Type> {
    let err = || syn::Error::new(output.span(), "expected a return type of `Result<T, E>`");
    let ReturnType::Type(_, ty) = output else {
        return Err(err());
    };
    let Type::Path(p) = &**ty else {
        return Err(err());
    };
    let seg = p.path.segments.last().ok_or_else(err)?;
    if seg.ident != "Result" {
        return Err(err());
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(err());
    };
    match args.args.first() {
        Some(GenericArgument::Type(t)) => Ok(t.clone()),
        _ => Err(err()),
    }
}

pub fn typed(arg: &FnArg) -> syn::Result<(&PatType, Ident)> {
    let FnArg::Typed(pt) = arg else {
        return Err(syn::Error::new(
            arg.span(),
            "methods are not supported; use a free function",
        ));
    };
    match &*pt.pat {
        Pat::Ident(i) if i.by_ref.is_none() => Ok((pt, i.ident.clone())),
        other => Err(syn::Error::new(
            other.span(),
            "use a plain identifier pattern (e.g. `user: Signup`)",
        )),
    }
}

/// `publish_video` to `PublishVideo`.
pub fn pascal(name: &str) -> String {
    name.trim_start_matches("r#")
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut c = s.chars();
            c.next()
                .map_or_else(String::new, |f| f.to_uppercase().chain(c).collect())
        })
        .collect()
}

/// Path used in generated code; override with `crate = "..."` on the attribute.
pub fn default_crate() -> TokenStream {
    quote! { ::durable_workflow }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pascal_cases_function_names() {
        assert_eq!(pascal("publish_video"), "PublishVideo");
        assert_eq!(pascal("onboarding"), "Onboarding");
        assert_eq!(pascal("r#type_check"), "TypeCheck");
        assert_eq!(pascal("a__b"), "AB");
    }

    #[test]
    fn extracts_dep_and_result_types() {
        let t: Type = syn::parse_quote!(durable_workflow::Dep<dyn Mailer>);
        assert!(dep_inner(&t).is_some());
        let t: Type = syn::parse_quote!(Vec<u8>);
        assert!(dep_inner(&t).is_none());
        let out: ReturnType = syn::parse_quote!(-> Result<Vec<u8>, StepError>);
        let ok = result_ok(&out).unwrap();
        assert_eq!(quote!(#ok).to_string(), "Vec < u8 >");
        let bad: ReturnType = syn::parse_quote!(-> u8);
        assert!(result_ok(&bad).is_err());
    }
}
