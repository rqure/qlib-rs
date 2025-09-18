use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use qlib_rs::*;
use qlib_rs::data::{EntityType, StorageScope};

// Helper to create an entity schema with basic fields
fn create_entity_schema(store: &mut Store, entity_type: EntityType) -> Result<()> {
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), vec![]);
    let ft_name = FieldType::from("Name");
    let ft_parent = FieldType::from("Parent");
    let ft_children = FieldType::from("Children");
    let ft_score = FieldType::from("Score");
    let ft_active = FieldType::from("Active");

    schema.fields.insert(ft_name.clone(), FieldSchema::String {
        field_type: ft_name.clone(),
        default_value: String::new(),
        rank: 0,
        storage_scope: StorageScope::Runtime,
    });

    schema.fields.insert(ft_parent.clone(), FieldSchema::EntityReference {
        field_type: ft_parent.clone(),
        default_value: None,
        rank: 1,
        storage_scope: StorageScope::Runtime,
    });

    schema.fields.insert(ft_children.clone(), FieldSchema::EntityList {
        field_type: ft_children.clone(),
        default_value: Vec::new(),
        rank: 2,
        storage_scope: StorageScope::Runtime,
    });

    schema.fields.insert(ft_score.clone(), FieldSchema::Int {
        field_type: ft_score.clone(),
        default_value: 0,
        rank: 3,
        storage_scope: StorageScope::Runtime,
    });

    schema.fields.insert(ft_active.clone(), FieldSchema::Bool {
        field_type: ft_active.clone(),
        default_value: true,
        rank: 4,
        storage_scope: StorageScope::Runtime,
    });

    let requests = vec![sschemaupdate!(schema)];
    store.perform_mut(requests)?;
    Ok(())
}

fn bench_entity_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_creation");
    
    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(BenchmarkId::new("batch_create", batch_size), batch_size, |b, &batch_size| {
            b.iter(|| {
                let mut store = Store::new(Snowflake::new());
                let et_user = EntityType::from("User");
                
                create_entity_schema(&mut store, &et_user).unwrap();
                
                let mut create_requests = Vec::new();
                for i in 0..batch_size {
                    create_requests.push(screate!(et_user.clone(), format!("User{}", i)));
                }
                
                black_box(store.perform_mut(create_requests).unwrap());
            })
        });
    }
    
    group.finish();
}

fn bench_field_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_operations");
    
    // Pre-create a store with entities for field operations
    let (mut store, entity_ids) = {
        let mut store = Store::new(Snowflake::new());
        let et_user = EntityType::from("User");
        
        create_entity_schema(&mut store, &et_user).unwrap();
        
        let mut create_requests = Vec::new();
        for i in 0..1000 {
            create_requests.push(screate!(et_user.clone(), format!("User{}", i)));
        }
        let create_requests = store.perform_mut(create_requests).unwrap();
        
        let entity_ids: Vec<EntityId> = create_requests
            .iter()
            .filter_map(|req| req.entity_id())
            .cloned()
            .collect();
            
        (store, entity_ids)
    };
    
    for op_count in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*op_count as u64));
        
        group.bench_with_input(BenchmarkId::new("bulk_write", op_count), op_count, |b, &op_count| {
            let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();
            
            b.iter(|| {
                let mut write_requests = Vec::new();
                for (i, entity_id) in entity_subset.iter().enumerate() {
                    write_requests.push(swrite!(entity_id, "Score".into(), sint!(i as i64)));
                }
                black_box(store.perform_mut(write_requests).unwrap());
            })
        });
        
        group.bench_with_input(BenchmarkId::new("bulk_read", op_count), op_count, |b, &op_count| {
            let entity_subset: Vec<_> = entity_ids.iter().take(op_count).cloned().collect();
            
            b.iter(|| {
                let mut read_requests = Vec::new();
                for entity_id in &entity_subset {
                    read_requests.push(sread!(entity_id, "Name".into()));
                }
                black_box(store.perform_mut(read_requests).unwrap());
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
                let mut store = Store::new(Snowflake::new());
                let et_user = EntityType::from("User");
                
                create_entity_schema(&mut store, &et_user).unwrap();
                
                let mut create_requests = Vec::new();
                for i in 0..dataset_size {
                    create_requests.push(screate!(et_user.clone(), format!("User{:04}", i)));
                }
                store.perform_mut(create_requests).unwrap();
                
                store
            };
            
            b.iter(|| {
                let et_user = EntityType::from("User");
                black_box(store.find_entities(&et_user, None).unwrap());
            })
        });
        
        group.bench_with_input(BenchmarkId::new("find_entities_paginated", dataset_size), dataset_size, |b, &dataset_size| {
            let store = {
                let mut store = Store::new(Snowflake::new());
                let et_user = EntityType::from("User");
                
                create_entity_schema(&mut store, &et_user).unwrap();
                
                let mut create_requests = Vec::new();
                for i in 0..dataset_size {
                    create_requests.push(screate!(et_user.clone(), format!("User{:04}", i)));
                }
                store.perform_mut(create_requests).unwrap();
                
                store
            };

            b.iter(|| {
                let et_user = EntityType::from("User");
                let page_opts = PageOpts::new(100, None);
                black_box(store.find_entities_paginated(&et_user, Some(page_opts), None).unwrap());
            })
        });
    }
    
    group.finish();
}

fn bench_inheritance_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("inheritance_operations");
    
    // Create inheritance hierarchy: Entity -> User -> AdminUser
    let store = {
        let mut store = Store::new(Snowflake::new());
        
        let et_entity = EntityType::from("Entity");
        let et_user = EntityType::from("User");
        let et_admin = EntityType::from("AdminUser");
        
        create_entity_schema(&mut store, &et_entity).unwrap();
        
        // User inherits from Entity
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), vec![et_entity.clone()]);
        user_schema.fields.insert(
            FieldType::from("Email"),
            FieldSchema::String {
                field_type: FieldType::from("Email"),
                default_value: String::new(),
                rank: 10,
                storage_scope: StorageScope::Runtime,
            }
        );
        let requests = vec![sschemaupdate!(user_schema)];
        store.perform_mut(requests).unwrap();
        
        // AdminUser inherits from User
        let mut admin_schema = EntitySchema::<Single>::new(et_admin.clone(), vec![et_user.clone()]);
        admin_schema.fields.insert(
            FieldType::from("AdminLevel"),
            FieldSchema::Int {
                field_type: FieldType::from("AdminLevel"),
                default_value: 1,
                rank: 20,
                storage_scope: StorageScope::Runtime,
            }
        );
        let requests = vec![sschemaupdate!(admin_schema)];
        store.perform_mut(requests).unwrap();
        
        // Create entities
        let mut create_requests = Vec::new();
        for i in 0..1000 {
            create_requests.push(screate!(et_entity.clone(), format!("Entity{}", i)));
            create_requests.push(screate!(et_user.clone(), format!("User{}", i)));
            create_requests.push(screate!(et_admin.clone(), format!("Admin{}", i)));
        }
        store.perform_mut(create_requests).unwrap();
        
        store
    };

    group.bench_function("find_with_inheritance", |b| {
        b.iter(|| {
            let et_entity = EntityType::from("Entity");
            black_box(store.find_entities(&et_entity, None).unwrap());
        })
    });
    
    group.bench_function("find_exact_type", |b| {
        b.iter(|| {
            let et_entity = EntityType::from("Entity");
            black_box(store.find_entities_exact(&et_entity, None, None).unwrap());
        })
    });
    
    group.bench_function("get_complete_schema", |b| {
        b.iter(|| {
            let et_admin = EntityType::from("AdminUser");
            black_box(store.get_complete_entity_schema(&et_admin).unwrap());
        })
    });
    
    group.finish();
}

fn bench_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pagination");
    
    // Create a large dataset
    let store = {
        let mut store = Store::new(Snowflake::new());
        let et_user = EntityType::from("User");
        
        create_entity_schema(&mut store, &et_user).unwrap();
        
        let mut create_requests = Vec::new();
        for i in 0..10000 {
            create_requests.push(screate!(et_user.clone(), format!("User{:05}", i)));
        }
        store.perform_mut(create_requests).unwrap();
        
        store
    };

    for page_size in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("page_size", page_size), page_size, |b, &page_size| {
            b.iter(|| {
                let et_user = EntityType::from("User");
                let page_opts = PageOpts::new(page_size, None);
                black_box(store.find_entities_paginated(&et_user, Some(page_opts), None).unwrap());
            })
        });
    }
    
    group.finish();
}

fn bench_schema_operations(c: &mut Criterion) {    
    let mut group = c.benchmark_group("schema_operations");
    
    group.bench_function("schema_creation", |b| {
        b.iter(|| {
            let mut store = Store::new(Snowflake::new());
            let et_test = EntityType::from("TestEntity");
            black_box(create_entity_schema(&mut store, &et_test).unwrap());
        })
    });
    
    // Pre-create a store with schemas for retrieval benchmarks
    let store = {
        let mut store = Store::new(Snowflake::new());
        for i in 0..100 {
            let et = EntityType::from(format!("TestEntity{}", i));
            create_entity_schema(&mut store, &et).unwrap();
        }
        store
    };

    group.bench_function("schema_retrieval", |b| {
        b.iter(|| {
            let et_test = EntityType::from("TestEntity50");
            black_box(store.get_entity_schema(&et_test).unwrap());
        })
    });
    
    group.bench_function("complete_schema_retrieval", |b| {
        b.iter(|| {
            let et_test = EntityType::from("TestEntity50");
            black_box(store.get_complete_entity_schema(&et_test).unwrap());
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
    bench_schema_operations
);

criterion_main!(benches);