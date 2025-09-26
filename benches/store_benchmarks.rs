use std::vec;

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use qlib_rs::data::StorageScope;
use qlib_rs::*;
use qlib_rs::{decode_message, QrespCodec, QrespMessage};

// Helper to create an entity schema with basic fields
fn create_entity_schema_with_name(store: &mut Store, entity_type_name: &str) -> Result<()> {
    // Create schema using strings first - perform_mut will intern the types
    let mut schema =
        EntitySchema::<Single, String, String>::new(entity_type_name.to_string(), vec![]);

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

    let requests = sreq![sschemaupdate!(schema)];
    store.perform_mut(requests)?;
    Ok(())
}

fn build_serialization_message(batch_size: usize) -> StoreMessage {
    let entity_type = EntityType(1);
    let value_field = FieldType(1);
    let aux_field = FieldType(2);

    let mut requests_vec = Vec::with_capacity(batch_size.saturating_mul(2).max(1));

    for i in 0..batch_size {
        let entity_id = EntityId::new(entity_type, i as u32 + 1);
        requests_vec.push(swrite!(entity_id, sfield![value_field], sint!(i as i64)));
        requests_vec.push(sread!(entity_id, sfield![aux_field]));
    }

    if batch_size == 0 {
        requests_vec.push(sread!(EntityId::new(entity_type, 0), sfield![value_field]));
    }

    let requests = Requests::new(requests_vec);
    StoreMessage::Perform { id: 42, requests }
}

fn bench_store_message_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_message_serialization");
    let batch_sizes = [1usize, 8, 64, 256];

    for &batch_size in &batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));

        let message = build_serialization_message(batch_size);

        group.bench_function(BenchmarkId::new("encode", batch_size), |b| {
            b.iter(|| black_box(encode_store_message(black_box(&message)).unwrap()))
        });

        let encoded = encode_store_message(&message).unwrap();

        group.bench_function(BenchmarkId::new("decode", batch_size), |b| {
            b.iter(|| {
                let mut buffer = BytesMut::from(black_box(encoded.as_slice()));
                let frame = QrespCodec::decode(&mut buffer).unwrap().unwrap();
                match decode_message(frame).unwrap() {
                    QrespMessage::Store(store_message) => {
                        black_box(store_message);
                    }
                    _ => unreachable!("expected store message"),
                }
            })
        });
        
        // Zero-copy decoding benchmark
        group.bench_function(BenchmarkId::new("decode_zero_copy", batch_size), |b| {
            b.iter(|| {
                let data = black_box(encoded.as_slice());
                let (frame_ref, _consumed) = QrespCodec::decode_ref(data).unwrap().unwrap();
                // Convert to owned only when necessary for full comparison
                let frame = frame_ref.to_owned();
                match decode_message(frame).unwrap() {
                    QrespMessage::Store(store_message) => {
                        black_box(store_message);
                    }
                    _ => unreachable!("expected store message"),
                }
            })
        });
    }

    group.finish();
}

fn bench_zero_copy_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_copy_parsing");
    
    // Create test data with different frame types
    let test_cases = vec![
        ("bulk_string", b"$11\r\nhello world\r\n".to_vec()),
        ("integer", b":12345\r\n".to_vec()),
        ("simple_string", b"+hello\r\n".to_vec()),
        ("boolean_true", b"#1\r\n".to_vec()),
        ("array", b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_vec()),
    ];
    
    for (name, data) in test_cases {
        group.throughput(Throughput::Bytes(data.len() as u64));
        
        // Traditional owned parsing
        group.bench_function(BenchmarkId::new("owned_parse", name), |b| {
            b.iter(|| {
                let mut buffer = BytesMut::from(black_box(data.as_slice()));
                let frame = QrespCodec::decode(&mut buffer).unwrap().unwrap();
                black_box(frame);
            })
        });
        
        // Zero-copy parsing
        group.bench_function(BenchmarkId::new("zero_copy_parse", name), |b| {
            b.iter(|| {
                let data_slice = black_box(data.as_slice());
                let (frame_ref, _consumed) = QrespCodec::decode_ref(data_slice).unwrap().unwrap();
                black_box(frame_ref);
            })
        });
        
        // Zero-copy with minimal conversion
        group.bench_function(BenchmarkId::new("zero_copy_extract", name), |b| {
            b.iter(|| {
                let data_slice = black_box(data.as_slice());
                let (frame_ref, _consumed) = QrespCodec::decode_ref(data_slice).unwrap().unwrap();
                // Only extract the data we need without full conversion
                let result = match &frame_ref {
                    qlib_rs::qresp::QrespFrameRef::Bulk(bytes) => bytes.len(),
                    qlib_rs::qresp::QrespFrameRef::Integer(i) => *i as usize,
                    qlib_rs::qresp::QrespFrameRef::Simple(s) => s.len(),
                    qlib_rs::qresp::QrespFrameRef::Boolean(b) => if *b { 1 } else { 0 },
                    qlib_rs::qresp::QrespFrameRef::Array(items) => items.len(),
                    _ => 0,
                };
                black_box(result);
            })
        });
    }
    
    group.finish();
}

fn bench_entity_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_creation");

    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("batch_create", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let mut store = Store::new();

                    create_entity_schema_with_name(&mut store, "User").unwrap();
                    let et_user = store.get_entity_type("User").unwrap();

                    let create_requests = Requests::new(vec![]);
                    for i in 0..batch_size {
                        create_requests.push(screate!(et_user, format!("User{}", i)));
                    }

                    black_box(store.perform_mut(create_requests).unwrap());
                })
            },
        );
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

        let create_requests = Requests::new(vec![]);
        for i in 0..1000 {
            create_requests.push(screate!(et_user, format!("User{}", i)));
        }
        let create_requests = store.perform_mut(create_requests).unwrap();

        let entity_ids: Vec<EntityId> = create_requests
            .read()
            .iter()
            .filter_map(|req| {
                if let Request::Create {
                    created_entity_id: Some(id),
                    ..
                } = req
                {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        (store, entity_ids)
    };

    let ft_score = store.get_field_type("Score").unwrap();
    let ft_name = store.get_field_type("Name").unwrap();

    for op_count in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*op_count as u64));

        group.bench_with_input(
            BenchmarkId::new("bulk_write", op_count),
            op_count,
            |b, &op_count| {
                let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();

                b.iter(|| {
                    let write_requests = Requests::new(vec![]);
                    for (i, entity_id) in entity_subset.iter().enumerate() {
                        write_requests.push(swrite!(
                            *entity_id,
                            crate::sfield![ft_score],
                            sint!(i as i64)
                        ));
                    }
                    black_box(store.perform_mut(write_requests).unwrap());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("bulk_read", op_count),
            op_count,
            |b, &op_count| {
                let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();

                b.iter(|| {
                    let read_requests = Requests::new(vec![]);
                    for entity_id in &entity_subset {
                        read_requests.push(sread!(*entity_id, crate::sfield![ft_name]));
                    }
                    black_box(store.perform(read_requests).unwrap());
                })
            },
        );
    }

    group.finish();
}

fn bench_entity_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_search");

    for dataset_size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*dataset_size as u64));

        group.bench_with_input(
            BenchmarkId::new("find_entities", dataset_size),
            dataset_size,
            |b, &dataset_size| {
                let store = {
                    let mut store = Store::new();

                    create_entity_schema_with_name(&mut store, "User").unwrap();
                    let et_user = store.get_entity_type("User").unwrap();

                    let create_requests = Requests::new(vec![]);
                    for i in 0..dataset_size {
                        create_requests.push(screate!(et_user, format!("User{:04}", i)));
                    }
                    store.perform_mut(create_requests).unwrap();

                    store
                };

                b.iter(|| {
                    let et_user = store.get_entity_type("User").unwrap();
                    black_box(store.find_entities(et_user, None).unwrap());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("find_entities_paginated", dataset_size),
            dataset_size,
            |b, &dataset_size| {
                let store = {
                    let mut store = Store::new();

                    create_entity_schema_with_name(&mut store, "User").unwrap();
                    let et_user = store.get_entity_type("User").unwrap();

                    let create_requests = Requests::new(vec![]);
                    for i in 0..dataset_size {
                        create_requests.push(screate!(et_user, format!("User{:04}", i)));
                    }
                    store.perform_mut(create_requests).unwrap();

                    store
                };

                b.iter(|| {
                    let et_user = store.get_entity_type("User").unwrap();
                    let page_opts = PageOpts::new(100, None);
                    black_box(
                        store
                            .find_entities_paginated(et_user, Some(&page_opts), None)
                            .unwrap(),
                    );
                })
            },
        );
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
        let mut user_schema = EntitySchema::<Single, String, String>::new(
            "User".to_string(),
            vec!["Entity".to_string()],
        );
        user_schema.fields.insert(
            "Email".to_string(),
            FieldSchema::String {
                field_type: "Email".to_string(),
                default_value: String::new(),
                rank: 10,
                storage_scope: StorageScope::Runtime,
            },
        );
        let requests = sreq![sschemaupdate!(user_schema)];
        store.perform_mut(requests).unwrap();

        // AdminUser inherits from User
        let mut admin_schema = EntitySchema::<Single, String, String>::new(
            "AdminUser".to_string(),
            vec!["User".to_string()],
        );
        admin_schema.fields.insert(
            "AdminLevel".to_string(),
            FieldSchema::Int {
                field_type: "AdminLevel".to_string(),
                default_value: 1,
                rank: 20,
                storage_scope: StorageScope::Runtime,
            },
        );
        let requests = sreq![sschemaupdate!(admin_schema)];
        store.perform_mut(requests).unwrap();

        // Get the interned entity types
        let et_entity = store.get_entity_type("Entity").unwrap();
        let et_user = store.get_entity_type("User").unwrap();
        let et_admin = store.get_entity_type("AdminUser").unwrap();

        // Create entities
        let create_requests = Requests::new(vec![]);
        for i in 0..1000 {
            create_requests.push(screate!(et_entity, format!("Entity{}", i)));
            create_requests.push(screate!(et_user, format!("User{}", i)));
            create_requests.push(screate!(et_admin, format!("Admin{}", i)));
        }
        store.perform_mut(create_requests).unwrap();

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

        let create_requests = Requests::new(vec![]);
        for i in 0..10000 {
            create_requests.push(screate!(et_user, format!("User{:05}", i)));
        }
        store.perform_mut(create_requests).unwrap();

        store
    };

    for page_size in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("page_size", page_size),
            page_size,
            |b, &page_size| {
                b.iter(|| {
                    let et_user = store.get_entity_type("User").unwrap();
                    let page_opts = PageOpts::new(page_size, None);
                    black_box(
                        store
                            .find_entities_paginated(et_user, Some(&page_opts), None)
                            .unwrap(),
                    );
                })
            },
        );
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

criterion_group!(
    benches,
    bench_store_message_serialization,
    bench_zero_copy_parsing,
    bench_entity_creation,
    bench_field_operations,
    bench_entity_search,
    bench_inheritance_operations,
    bench_pagination,
    bench_schema_operations
);

criterion_main!(benches);
