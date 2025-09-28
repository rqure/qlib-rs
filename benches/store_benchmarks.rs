use std::vec;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use qlib_rs::*;
use qlib_rs::data::StorageScope;

// Helper to create an entity schema with basic fields
fn create_entity_schema_with_name(store: &mut Store, entity_type_name: &str) -> Result<()> {
    // Create schema using strings first - perform_mut will intern the types
    let mut schema = EntitySchema::<Single, String, String>::new(entity_type_name.to_string(), vec![]);
    
    schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        },
    );

    schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        },
    );

    schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        },
    );

    schema.fields.insert(
        "Score".to_string(),
        FieldSchema::Int {
            field_type: "Score".to_string(),
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        },
    );

    schema.fields.insert(
        "Active".to_string(),
        FieldSchema::Bool {
            field_type: "Active".to_string(),
            default_value: true,
            rank: 4,
            storage_scope: StorageScope::Runtime,
        },
    );

        store.update_schema(schema)?;
    Ok(())
}

fn bench_entity_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_creation");
    
    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(BenchmarkId::new("batch_create", batch_size), batch_size, |b, &batch_size| {
            b.iter(|| {
                let mut store = Store::new();
                
                create_entity_schema_with_name(&mut store, "User").unwrap();
                let et_user = store.get_entity_type("User").unwrap();
                
                for i in 0..batch_size {
                    black_box(store.create_entity(et_user, None, &format!("User{}", i)).unwrap());
                }
            })
        });
    }
    
    group.finish();
}

fn bench_field_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_operations");
    
    // Pre-create a store with entities for field operations
    let (mut store, entity_ids) = {
        let mut store = Store::new();
        
        create_entity_schema_with_name(&mut store, "User").unwrap();
        let et_user = store.get_entity_type("User").unwrap();
        
        let mut entity_ids = Vec::new();
        for i in 0..1000 {
            let entity_id = store.create_entity(et_user, None, &format!("User{}", i)).unwrap();
            entity_ids.push(entity_id);
        }
            
        (store, entity_ids)
    };
    
    let ft_score = store.get_field_type("Score").unwrap();
    let ft_name = store.get_field_type("Name").unwrap();
    
    for op_count in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*op_count as u64));
        
        group.bench_with_input(BenchmarkId::new("bulk_write", op_count), op_count, |b, &op_count| {
            let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();
            
            b.iter(|| {
                for (i, entity_id) in entity_subset.iter().enumerate() {
                    black_box(store.write(*entity_id, &[ft_score], Value::Int(i as i64), None, None, None, None).unwrap());
                }
            })
        });
        
        group.bench_with_input(BenchmarkId::new("bulk_read", op_count), op_count, |b, &op_count| {
            let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();
            
            b.iter(|| {
                for entity_id in &entity_subset {
                    black_box(store.read(*entity_id, &[ft_name]).unwrap());
                }
            })
        });
    }
    
    group.finish();
}

fn bench_entity_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_search");
    
    for dataset_size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*dataset_size as u64));
        
        group.bench_with_input(BenchmarkId::new("find_entities", dataset_size), dataset_size, |b, &dataset_size| {
            let store = {
                let mut store = Store::new();
                
                create_entity_schema_with_name(&mut store, "User").unwrap();
                let et_user = store.get_entity_type("User").unwrap();
                
                for i in 0..dataset_size {
                    store.create_entity(et_user, None, &format!("User{:04}", i)).unwrap();
                }
                
                store
            };
            
            b.iter(|| {
                let et_user = store.get_entity_type("User").unwrap();
                black_box(store.find_entities(et_user, None).unwrap());
            })
        });
        
        group.bench_with_input(BenchmarkId::new("find_entities_paginated", dataset_size), dataset_size, |b, &dataset_size| {
            let store = {
                let mut store = Store::new();
                
                create_entity_schema_with_name(&mut store, "User").unwrap();
                let et_user = store.get_entity_type("User").unwrap();
                
                for i in 0..dataset_size {
                    store.create_entity(et_user, None, &format!("User{:04}", i)).unwrap();
                }
                
                store
            };

            b.iter(|| {
                let et_user = store.get_entity_type("User").unwrap();
                let page_opts = PageOpts::new(100, None);
                black_box(store.find_entities_paginated(et_user, Some(&page_opts), None).unwrap());
            })
        });
    }
    
    group.finish();
}

fn bench_inheritance_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("inheritance_operations");
    
    // Create inheritance hierarchy: Entity -> User -> AdminUser
    let store = {
        let mut store = Store::new();
        
        // Create Entity schema
        create_entity_schema_with_name(&mut store, "Entity").unwrap();
        
        // User inherits from Entity
        let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec!["Entity".to_string()]);
        user_schema.fields.insert(
            "Email".to_string(),
            FieldSchema::String {
                field_type: "Email".to_string(),
                default_value: String::new(),
                rank: 10,
                storage_scope: StorageScope::Runtime,
            }
        );
        store.update_schema(user_schema).unwrap();
        
        // AdminUser inherits from User
        let mut admin_schema = EntitySchema::<Single, String, String>::new("AdminUser".to_string(), vec!["User".to_string()]);
        admin_schema.fields.insert(
            "AdminLevel".to_string(),
            FieldSchema::Int {
                field_type: "AdminLevel".to_string(),
                default_value: 1,
                rank: 20,
                storage_scope: StorageScope::Runtime,
            }
        );
        store.update_schema(admin_schema).unwrap();
        
        // Get the interned entity types
        let et_entity = store.get_entity_type("Entity").unwrap();
        let et_user = store.get_entity_type("User").unwrap();
        let et_admin = store.get_entity_type("AdminUser").unwrap();
        
        // Create entities
        for i in 0..1000 {
            store.create_entity(et_entity, None, &format!("Entity{}", i)).unwrap();
            store.create_entity(et_user, None, &format!("User{}", i)).unwrap();
            store.create_entity(et_admin, None, &format!("Admin{}", i)).unwrap();
        }
        
        store
    };

    group.bench_function("find_with_inheritance", |b| {
        b.iter(|| {
            let et_entity = store.get_entity_type("Entity").unwrap();
            black_box(store.find_entities(et_entity, None).unwrap());
        })
    });
    
    group.bench_function("find_exact_type", |b| {
        b.iter(|| {
            let et_entity = store.get_entity_type("Entity").unwrap();
            black_box(store.find_entities_exact(et_entity, None, None).unwrap());
        })
    });
    
    group.bench_function("get_complete_schema", |b| {
        b.iter(|| {
            let et_admin = store.get_entity_type("AdminUser").unwrap();
            black_box(store.get_complete_entity_schema(et_admin).unwrap());
        })
    });
    
    group.finish();
}

fn bench_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pagination");
    
    // Create a large dataset
    let store = {
        let mut store = Store::new();
        
        create_entity_schema_with_name(&mut store, "User").unwrap();
        let et_user = store.get_entity_type("User").unwrap();
        
        for i in 0..10000 {
            store.create_entity(et_user, None, &format!("User{:05}", i)).unwrap();
        }
        
        store
    };

    for page_size in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("page_size", page_size), page_size, |b, &page_size| {
            b.iter(|| {
                let et_user = store.get_entity_type("User").unwrap();
                let page_opts = PageOpts::new(page_size, None);
                black_box(store.find_entities_paginated(et_user, Some(&page_opts), None).unwrap());
            })
        });
    }
    
    group.finish();
}

fn bench_schema_operations(c: &mut Criterion) {    
    let mut group = c.benchmark_group("schema_operations");
    
    group.bench_function("schema_creation", |b| {
        b.iter(|| {
            let mut store = Store::new();
            black_box(create_entity_schema_with_name(&mut store, "TestEntity").unwrap());
        })
    });
    
    // Pre-create a store with schemas for retrieval benchmarks
    let store = {
        let mut store = Store::new();
        for i in 0..100 {
            let entity_name = format!("TestEntity{}", i);
            create_entity_schema_with_name(&mut store, &entity_name).unwrap();
        }
        store
    };

    group.bench_function("schema_retrieval", |b| {
        b.iter(|| {
            let et_test = store.get_entity_type("TestEntity50").unwrap();
            black_box(store.get_entity_schema(et_test).unwrap());
        })
    });
    
    group.bench_function("complete_schema_retrieval", |b| {
        b.iter(|| {
            let et_test = store.get_entity_type("TestEntity50").unwrap();
            black_box(store.get_complete_entity_schema(et_test).unwrap());
        })
    });
    
    group.finish();
}

fn bench_protocol_operations(c: &mut Criterion) {
    use qlib_rs::protocol::*;
    use bytes::Bytes;
    
    let mut group = c.benchmark_group("protocol");
    group.throughput(Throughput::Elements(1));
    
    // Create test commands for benchmarking
    let simple_cmd = QuspCommand::new("GET_ENTITY_TYPE", vec![
        Bytes::from_static(b"TestEntity"),
    ]);
    
    let complex_cmd = QuspCommand::new("FIND_ENTITIES_PAGINATED", vec![
        Bytes::from_static(b"1"),
        Bytes::from_static(b"null"), 
        Bytes::from_static(b"test_filter"),
    ]);
    
    let _write_cmd = QuspCommand::new("WRITE", vec![
        Bytes::from_static(b"123"),
        Bytes::from_static(b"1,2,3"),
        Bytes::from_static(b"test_value"),
        Bytes::from_static(b"null"),
        Bytes::from_static(b"null"),
        Bytes::from_static(b"null"),
        Bytes::from_static(b"null"),
    ]);
    
    // Benchmark command parsing with new refactored approach
    group.bench_function("parse_simple_command", |b| {
        b.iter(|| {
            black_box(parse_store_command(&simple_cmd))
        })
    });
    
    group.bench_function("parse_complex_command", |b| {
        b.iter(|| {
            black_box(parse_store_command(&complex_cmd))
        })
    });
    
    // Benchmark individual command parsers (should be faster than old monolithic approach)
    group.bench_function("parse_direct_simple", |b| {
        b.iter(|| {
            black_box(command_parsers::parse_get_entity_type(&simple_cmd))
        })
    });
    
    group.bench_function("parse_direct_complex", |b| {
        b.iter(|| {
            black_box(command_parsers::parse_find_entities_paginated(&complex_cmd))
        })
    });
    
    // Benchmark trait-based encoding
    group.bench_function("encode_integer_trait", |b| {
        let value: i64 = 12345;
        b.iter(|| {
            black_box(value.encode())
        })
    });
    
    group.bench_function("encode_string_trait", |b| {
        let value = "Hello, World!";
        b.iter(|| {
            black_box(value.encode())
        })
    });
    
    group.bench_function("encode_vec_trait", |b| {
        let values = vec![1i64, 2i64, 3i64, 4i64, 5i64];
        b.iter(|| {
            black_box(values.encode())
        })
    });
    
    // Compare with old approach
    group.bench_function("encode_integer_old", |b| {
        let value: i64 = 12345;
        b.iter(|| {
            black_box(encode_integer(value))
        })
    });
    
    group.bench_function("encode_string_old", |b| {
        let value = "Hello, World!";
        b.iter(|| {
            black_box(encode_simple_string(value))
        })
    });
    
    // Benchmark command argument validation
    group.bench_function("argument_validation", |b| {
        b.iter(|| {
            black_box(simple_cmd.expect_args(1, "GET_ENTITY_TYPE"))
        })
    });
    
    // Benchmark response encoding with traits
    let response = QuspResponse::Integer(42);
    group.bench_function("encode_response_trait", |b| {
        b.iter(|| {
            black_box(response.encode())
        })
    });
    
    group.bench_function("encode_response_old", |b| {
        b.iter(|| {
            black_box(encode_response(&response))
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_entity_creation,
    bench_field_operations,
    bench_entity_search,
    bench_inheritance_operations,
    bench_pagination,
    bench_schema_operations,
    bench_protocol_operations
);

criterion_main!(benches);