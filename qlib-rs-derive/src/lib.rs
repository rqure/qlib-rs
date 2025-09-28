use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Index, parse::Parse, Token, Ident, Type, punctuated::Punctuated, LitStr};

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

// ============================================================================
// Command Definition Macro
// ============================================================================

/// Input for the resp_command! macro
struct RespCommandInput {
    command_name: LitStr,
    struct_name: Ident,
    fields: Punctuated<CommandField, Token![,]>,
    execute_block: syn::Block,
}

/// A field in a command definition
struct CommandField {
    name: Ident,
    ty: Type,
    optional: bool,
}

impl Parse for CommandField {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let optional = input.peek(Token![?]);
        if optional {
            input.parse::<Token![?]>()?;
        }
        
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;
        
        Ok(CommandField { name, ty, optional })
    }
}

impl Parse for RespCommandInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let command_name: LitStr = input.parse()?;
        input.parse::<Token![=>]>()?;
        let struct_name: Ident = input.parse()?;
        
        let content;
        syn::braced!(content in input);
        
        // Parse fields
        let mut fields = Punctuated::new();
        while !content.peek(syn::token::Brace) && !content.is_empty() {
            fields.push_value(content.parse::<CommandField>()?);
            if !content.peek(syn::token::Brace) && !content.is_empty() {
                fields.push_punct(content.parse::<Token![,]>()?);
            }
        }
        
        // Parse execute block
        let execute_block: syn::Block = content.parse()?;
        
        Ok(RespCommandInput {
            command_name,
            struct_name,
            fields,
            execute_block,
        })
    }
}

/// Macro for defining RESP commands with automatic encoding/decoding
/// 
/// # Example
/// 
/// ```rust,ignore
/// resp_command! {
///     "READ" => ReadCommand {
///         entity_id: EntityId,
///         field_path: Vec<FieldType>,
///         {
///             let (value, timestamp, writer_id) = store.read(self.entity_id, &self.field_path)?;
///             Ok(RespResponse::Array(vec![
///                 RespResponse::Bulk(value.encode()),
///                 RespResponse::Bulk(timestamp.to_string().into_bytes()),
///                 match writer_id {
///                     Some(id) => RespResponse::Integer(id.0 as i64),
///                     None => RespResponse::Null,
///                 },
///             ]))
///         }
///     }
/// }
/// ```
#[proc_macro]
pub fn resp_command(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as RespCommandInput);
    
    let command_name_str = &input.command_name;
    let struct_name = &input.struct_name;
    let execute_block = &input.execute_block;
    
    // Generate field definitions
    let field_definitions: Vec<_> = input.fields.iter().map(|field| {
        let name = &field.name;
        let ty = &field.ty;
        if field.optional {
            quote! { pub #name: Option<#ty> }
        } else {
            quote! { pub #name: #ty }
        }
    }).collect();
    
    // Generate decoding logic
    let field_decoders: Vec<_> = input.fields.iter().enumerate().map(|(i, field)| {
        let name = &field.name;
        let field_index = i + 1; // Skip command name
        if field.optional {
            quote! {
                let #name = if elements.len() > #field_index {
                    match &elements[#field_index] {
                        crate::data::resp::RespValue::Null => None,
                        element => {
                            let field_bytes = element.encode();
                            let (decoded_field, _) = crate::data::resp::RespDecode::decode(&field_bytes)?;
                            Some(decoded_field)
                        }
                    }
                } else {
                    None
                };
            }
        } else {
            quote! {
                let #name = if elements.len() > #field_index {
                    let field_bytes = elements[#field_index].encode();
                    let (decoded_field, _) = crate::data::resp::RespDecode::decode(&field_bytes)?;
                    decoded_field
                } else {
                    return Err(crate::Error::InvalidRequest(format!("Missing required field {}", stringify!(#name))));
                };
            }
        }
    }).collect();
    
    let field_names: Vec<_> = input.fields.iter().map(|f| &f.name).collect();
    
    // Generate encoding logic
    let field_encoders: Vec<_> = input.fields.iter().map(|field| {
        let name = &field.name;
        if field.optional {
            quote! {
                if let Some(ref value) = self.#name {
                    let field_bytes = value.encode();
                    let (field_value, _) = crate::data::resp::RespValue::decode(&field_bytes)
                        .map_err(|_| crate::Error::InvalidRequest("Failed to re-encode field".to_string()))?;
                    elements.push(field_value);
                } else {
                    elements.push(crate::data::resp::RespValue::Null);
                }
            }
        } else {
            quote! {
                let field_bytes = self.#name.encode();
                let (field_value, _) = crate::data::resp::RespValue::decode(&field_bytes)
                    .map_err(|_| crate::Error::InvalidRequest("Failed to re-encode field".to_string()))?;
                elements.push(field_value);
            }
        }
    }).collect();
    
    let expanded = quote! {
        /// Auto-generated RESP command
        #[derive(Debug, Clone)]
        pub struct #struct_name<'a> {
            #(#field_definitions,)*
            pub _marker: std::marker::PhantomData<&'a ()>,
        }
        
        impl<'a> crate::data::resp::RespDecode<'a> for #struct_name<'a> {
            fn decode(input: &'a [u8]) -> crate::Result<(Self, &'a [u8])> {
                let (array, remaining) = match crate::data::resp::RespValue::decode(input)? {
                    (crate::data::resp::RespValue::Array(arr), rem) => (arr, rem),
                    _ => return Err(crate::Error::InvalidRequest("Command must be an array".to_string())),
                };
                
                if array.is_empty() {
                    return Err(crate::Error::InvalidRequest("Empty command array".to_string()));
                }
                
                // Verify command name
                let command_name = match &array[0] {
                    crate::data::resp::RespValue::BulkString(data) => {
                        std::str::from_utf8(data)
                            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in command name".to_string()))?
                    },
                    crate::data::resp::RespValue::SimpleString(s) => s,
                    _ => return Err(crate::Error::InvalidRequest("Command name must be string".to_string())),
                };
                
                if !command_name.eq_ignore_ascii_case(#command_name_str) {
                    return Err(crate::Error::InvalidRequest(format!("Expected command {}, got {}", #command_name_str, command_name)));
                }
                
                // Decode fields
                #(#field_decoders)*
                
                Ok((Self {
                    #(#field_names,)*
                    _marker: std::marker::PhantomData,
                }, remaining))
            }
        }
        
        impl<'a> crate::data::resp::RespEncode for #struct_name<'a> {
            fn encode(&self) -> Vec<u8> {
                let mut elements = Vec::new();
                elements.push(crate::data::resp::RespValue::BulkString(#command_name_str.as_bytes()));
                
                #(#field_encoders)*
                
                let array = crate::data::resp::RespValue::Array(elements);
                array.encode()
            }
        }
        
        impl<'a> crate::data::resp::RespCommand<'a> for #struct_name<'a> {
            const COMMAND_NAME: &'static str = #command_name_str;
            
            fn execute(&self, store: &mut dyn crate::data::StoreTrait) -> crate::Result<crate::data::resp::RespResponse> {
                #execute_block
            }
        }
    };
    
    TokenStream::from(expanded)
}