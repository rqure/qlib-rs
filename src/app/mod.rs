use crossbeam::channel::{Receiver, Sender};

use crate::{et::ET, ft::FT, EntityId, Notification, NotifyConfig, Result, StoreProxy};

// Represents a logical component that can act as a candidate for leadership
// in a fault-tolerant setup. Typically this would be a Service, but could
// also be communication line (eg. Socket connection).
pub struct CandidateState {
    pub candidate_id: EntityId,
    pub is_leader: bool,

    notify_ch: (Sender<Notification>, Receiver<Notification>),
    et: ET,
    ft: FT
}

pub struct ServiceState {
    // The machine this service is running on
    pub machine_id: String,

    // Services need to send a heartbeat periodically to indicate they are alive
    pub service_id: EntityId,

    // Services can be fault tolerant (i.e. have a leader election mechanism)
    pub fault_tolerant: bool,

    // Candidate state for fault tolerance
    pub candidate_state: Option<CandidateState>,

    // Heartbeat management
    pub heartbeat_interval_msecs: u64,
    last_heartbeat: std::time::Instant,

    et: ET,
    ft: FT
}

impl ServiceState {
    pub fn new(store: &mut StoreProxy, service_name: String, fault_tolerant: bool, heartbeat_interval_msecs: u64) -> Result<Self> {
        let machine_id = store.machine_info()?;
        let et = ET::new(store);
        let ft = FT::new(store);

        let et_service = et.service.expect("Service entity type should be defined");
        let et_fault_tolerance = et.fault_tolerance.expect("FaultTolerance entity type should be defined");
        let ft_current_leader = ft.current_leader.expect("CurrentLeader field type should be defined");

        let service_id = {
            let query = format!("Parent->Name == '{}' && Name == '{}'", machine_id, service_name);
            let entities = store.find_entities(
                et_service,
                Some(query.as_str())
            )?;
            entities.get(0).expect("Service entity instance to exist").clone()
        };

        let notify_ch = crossbeam::channel::unbounded();
        if fault_tolerant {
            let fault_tolerance_id = {
                let query = format!("CandidateList.contains({})", String::from(service_id));
                let entities = store.find_entities(
                    et_fault_tolerance,
                    Some(query.as_str())
                )?;
                entities.get(0).expect("FaultTolerance entity instance to exist").clone()
            };

            store.register_notification(NotifyConfig::EntityId {
                entity_id: fault_tolerance_id,
                field_type: ft_current_leader,
                trigger_on_change: true,
                context: vec![]
            }, notify_ch.0.clone())?;
        }

        let candidate_state = if fault_tolerant {
            Some(CandidateState {
                candidate_id: service_id,
                is_leader: false,
                notify_ch,
                et: et.clone(),
                ft: ft.clone(),
            })
        } else {
            None
        };

        Ok(ServiceState {
            machine_id,
            service_id,
            fault_tolerant,
            candidate_state,
            heartbeat_interval_msecs,
            last_heartbeat: std::time::Instant::now(),
            et,
            ft,
        })
    }

    // Called back the main loop tick
    pub fn tick(&mut self, store: &mut StoreProxy) -> Result<()> {
        let et_service = self.et.service.expect("Service entity type should be defined");
        
        // Heartbeat update

        // Candidate tick

        Ok(())
    }
}

impl CandidateState {
    pub fn new(store: &mut StoreProxy, candidate_id: EntityId) -> Self {
        let et = ET::new(store);
        let ft = FT::new(store);
        let notify_ch = crossbeam::channel::unbounded();
        
        CandidateState {
            candidate_id,
            is_leader: false,
            notify_ch,
            et,
            ft,
        }
    }

    pub fn make_me_available(&mut self, store: &mut StoreProxy) -> Result<()> {
        // Writes the Candidate status as "Available", allowing it to be elected as leader

        Ok(())
    }

    pub fn make_me_unavailable(&mut self, store: &mut StoreProxy) -> Result<()> {
        // Writes the Candidate status as "Unavailable", preventing it from being elected as leader

        Ok(())
    }
}

