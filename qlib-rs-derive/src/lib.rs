use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Index, parse::Parse, Token, Ident, Type, punctuated::Punctuated, LitStr, braced};

/// Derive macro for `RespEncode` trait
#[proc_macro_derive(RespEncode)]
pub fn derive_resp_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let encode_impl = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    let field_encodes: Vec<_> = fields.named.iter().map(|field| {
                        let field_name = &field.ident;
                        quote! {
                            // Encode field name as a string identifier
                            elements.push(crate::data::resp::RespValue::BulkString(
                                stringify!(#field_name).as_bytes()
                            ));
                            // Encode field value
                            let field_encoded = crate::data::resp::RespEncode::encode(&self.#field_name);
                            let (field_value, _) = crate::data::resp::RespValue::decode(&field_encoded)
                                .map_err(|_| "Failed to re-decode field")?;
                            elements.push(field_value);
                        }
                    }).collect();
                    
                    quote! {
                        let mut elements = Vec::new();
                        #(#field_encodes)*
                        crate::data::resp::RespValue::Array(elements).encode()
                    }
                }
                Fields::Unnamed(fields) => {
                    let field_encodes: Vec<_> = fields.unnamed.iter().enumerate().map(|(i, _)| {
                        let index = Index::from(i);
                        quote! {
                            let field_encoded = crate::data::resp::RespEncode::encode(&self.#index);
                            let (field_value, _) = crate::data::resp::RespValue::decode(&field_encoded)
                                .map_err(|_| "Failed to re-decode field")?;
                            elements.push(field_value);
                        }
                    }).collect();
                    
                    quote! {
                        let mut elements = Vec::new();
                        #(#field_encodes)*
                        crate::data::resp::RespValue::Array(elements).encode()
                    }
                }
                Fields::Unit => {
                    quote! {
                        crate::data::resp::RespValue::Array(vec![]).encode()
                    }
                }
            }
        }
        Data::Enum(data) => {
            let variant_arms: Vec<_> = data.variants.iter().enumerate().map(|(i, variant)| {
                let variant_name = &variant.ident;
                let variant_index = i as u32;
                
                match &variant.fields {
                    Fields::Named(fields) => {
                        let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                        let field_encodes: Vec<_> = fields.named.iter().map(|field| {
                            let field_name = &field.ident;
                            quote! {
                                let field_encoded = crate::data::resp::RespEncode::encode(#field_name);
                                let (field_value, _) = crate::data::resp::RespValue::decode(&field_encoded)
                                    .map_err(|_| "Failed to re-decode field")?;
                                elements.push(field_value);
                            }
                        }).collect();
                        
                        quote! {
                            Self::#variant_name { #(#field_names),* } => {
                                let mut elements = vec![
                                    crate::data::resp::RespValue::Integer(#variant_index as i64),
                                ];
                                #(#field_encodes)*
                                crate::data::resp::RespValue::Array(elements).encode()
                            }
                        }
                    }
                    Fields::Unnamed(fields) => {
                        let field_names: Vec<_> = (0..fields.unnamed.len())
                            .map(|i| format_ident!("field_{}", i))
                            .collect();
                        let field_encodes: Vec<_> = field_names.iter().map(|field_name| {
                            quote! {
                                let field_encoded = crate::data::resp::RespEncode::encode(#field_name);
                                let (field_value, _) = crate::data::resp::RespValue::decode(&field_encoded)
                                    .map_err(|_| "Failed to re-decode field")?;
                                elements.push(field_value);
                            }
                        }).collect();
                        
                        quote! {
                            Self::#variant_name(#(#field_names),*) => {
                                let mut elements = vec![
                                    crate::data::resp::RespValue::Integer(#variant_index as i64),
                                ];
                                #(#field_encodes)*
                                crate::data::resp::RespValue::Array(elements).encode()
                            }
                        }
                    }
                    Fields::Unit => {
                        quote! {
                            Self::#variant_name => {
                                crate::data::resp::RespValue::Array(vec![
                                    crate::data::resp::RespValue::Integer(#variant_index as i64),
                                ]).encode()
                            }
                        }
                    }
                }
            }).collect();
            
            quote! {
                match self {
                    #(#variant_arms)*
                }
            }
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(&input, "RespEncode cannot be derived for unions")
                .to_compile_error()
                .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics crate::data::resp::RespEncode for #name #ty_generics #where_clause {
            fn encode(&self) -> Vec<u8> {
                // Fallback wrapper to handle any potential errors during encoding
                let result: std::result::Result<Vec<u8>, &'static str> = (|| -> std::result::Result<Vec<u8>, &'static str> {
                    Ok(#encode_impl)
                })();
                result.unwrap_or_else(|_| {
                    // Fallback encoding in case of error
                    crate::data::resp::RespValue::Error("Encoding failed").encode()
                })
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for `RespDecode` trait
#[proc_macro_derive(RespDecode)]
pub fn derive_resp_decode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let decode_impl = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    let field_decodes: Vec<_> = fields.named.iter().enumerate().map(|(i, field)| {
                        let field_name = &field.ident;
                        let field_index = i * 2 + 1; // Skip field name, get value
                        quote! {
                            let #field_name = if elements.len() > #field_index {
                                let field_bytes = crate::data::resp::RespEncode::encode(&elements[#field_index]);
                                let (decoded_field, _) = <_ as crate::data::resp::RespDecode>::decode(&field_bytes)?;
                                decoded_field
                            } else {
                                return Err(crate::Error::InvalidRequest(format!("Missing field {}", stringify!(#field_name))));
                            };
                        }
                    }).collect();
                    
                    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                    
                    quote! {
                        let (value, remaining) = crate::data::resp::RespValue::decode(input)?;
                        match value {
                            crate::data::resp::RespValue::Array(elements) => {
                                #(#field_decodes)*
                                Ok((Self { #(#field_names),* }, remaining))
                            }
                            _ => Err(crate::Error::InvalidRequest("Expected array for struct".to_string())),
                        }
                    }
                }
                Fields::Unnamed(fields) => {
                    let field_decodes: Vec<_> = (0..fields.unnamed.len()).map(|i| {
                        let field_name = format_ident!("field_{}", i);
                        quote! {
                            let #field_name = if elements.len() > #i {
                                let field_bytes = crate::data::resp::RespEncode::encode(&elements[#i]);
                                let (decoded_field, _) = <_ as crate::data::resp::RespDecode>::decode(&field_bytes)?;
                                decoded_field
                            } else {
                                return Err(crate::Error::InvalidRequest(format!("Missing field {}", #i)));
                            };
                        }
                    }).collect();
                    
                    let field_names: Vec<_> = (0..fields.unnamed.len())
                        .map(|i| format_ident!("field_{}", i))
                        .collect();
                    
                    quote! {
                        let (value, remaining) = crate::data::resp::RespValue::decode(input)?;
                        match value {
                            crate::data::resp::RespValue::Array(elements) => {
                                #(#field_decodes)*
                                Ok((Self(#(#field_names),*), remaining))
                            }
                            _ => Err(crate::Error::InvalidRequest("Expected array for tuple struct".to_string())),
                        }
                    }
                }
                Fields::Unit => {
                    quote! {
                        let (value, remaining) = crate::data::resp::RespValue::decode(input)?;
                        match value {
                            crate::data::resp::RespValue::Array(_) => Ok((Self, remaining)),
                            _ => Err(crate::Error::InvalidRequest("Expected array for unit struct".to_string())),
                        }
                    }
                }
            }
        }
        Data::Enum(data) => {
            let variant_arms: Vec<_> = data.variants.iter().enumerate().map(|(i, variant)| {
                let variant_name = &variant.ident;
                let variant_index = i as i64;
                
                match &variant.fields {
                    Fields::Named(fields) => {
                        let field_decodes: Vec<_> = fields.named.iter().enumerate().map(|(field_i, field)| {
                            let field_name = &field.ident;
                            let element_index = field_i + 1; // Skip variant discriminant
                            quote! {
                                let #field_name = if elements.len() > #element_index {
                                    let field_bytes = crate::data::resp::RespEncode::encode(&elements[#element_index]);
                                    let (decoded_field, _) = <_ as crate::data::resp::RespDecode>::decode(&field_bytes)?;
                                    decoded_field
                                } else {
                                    return Err(crate::Error::InvalidRequest(format!("Missing field {} for variant {}", stringify!(#field_name), stringify!(#variant_name))));
                                };
                            }
                        }).collect();
                        
                        let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                        
                        quote! {
                            #variant_index => {
                                #(#field_decodes)*
                                Ok(Self::#variant_name { #(#field_names),* })
                            }
                        }
                    }
                    Fields::Unnamed(fields) => {
                        let field_decodes: Vec<_> = (0..fields.unnamed.len()).map(|field_i| {
                            let field_name = format_ident!("field_{}", field_i);
                            let element_index = field_i + 1; // Skip variant discriminant
                            quote! {
                                let #field_name = if elements.len() > #element_index {
                                    let field_bytes = crate::data::resp::RespEncode::encode(&elements[#element_index]);
                                    let (decoded_field, _) = <_ as crate::data::resp::RespDecode>::decode(&field_bytes)?;
                                    decoded_field
                                } else {
                                    return Err(crate::Error::InvalidRequest(format!("Missing field {} for variant {}", #field_i, stringify!(#variant_name))));
                                };
                            }
                        }).collect();
                        
                        let field_names: Vec<_> = (0..fields.unnamed.len())
                            .map(|i| format_ident!("field_{}", i))
                            .collect();
                        
                        quote! {
                            #variant_index => {
                                #(#field_decodes)*
                                Ok(Self::#variant_name(#(#field_names),*))
                            }
                        }
                    }
                    Fields::Unit => {
                        quote! {
                            #variant_index => Ok(Self::#variant_name),
                        }
                    }
                }
            }).collect();
            
            quote! {
                let (value, remaining) = crate::data::resp::RespValue::decode(input)?;
                match value {
                    crate::data::resp::RespValue::Array(elements) if !elements.is_empty() => {
                        let discriminant = match &elements[0] {
                            crate::data::resp::RespValue::Integer(i) => *i,
                            crate::data::resp::RespValue::BulkString(s) => {
                                std::str::from_utf8(s)
                                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in discriminant".to_string()))?
                                    .parse::<i64>()
                                    .map_err(|_| crate::Error::InvalidRequest("Invalid discriminant format".to_string()))?
                            }
                            crate::data::resp::RespValue::SimpleString(s) => {
                                s.parse::<i64>()
                                    .map_err(|_| crate::Error::InvalidRequest("Invalid discriminant format".to_string()))?
                            }
                            _ => return Err(crate::Error::InvalidRequest("Invalid discriminant type".to_string())),
                        };
                        
                        let result = match discriminant {
                            #(#variant_arms)*
                            _ => return Err(crate::Error::InvalidRequest(format!("Unknown variant discriminant: {}", discriminant))),
                        };
                        
                        Ok((result?, remaining))
                    }
                    _ => Err(crate::Error::InvalidRequest("Expected non-empty array for enum".to_string())),
                }
            }
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(&input, "RespDecode cannot be derived for unions")
                .to_compile_error()
                .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics crate::data::resp::RespDecode<'_> for #name #ty_generics #where_clause {
            fn decode(input: &[u8]) -> crate::Result<(Self, &[u8])> {
                #decode_impl
            }
        }
    };

    TokenStream::from(expanded)
}
