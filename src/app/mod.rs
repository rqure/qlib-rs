use crossbeam::channel::{Receiver, Sender};

use crate::{et::ET, ft::FT, EntityId, Notification, NotifyConfig, Result, StoreProxy, Value};

/// Represents a logical component that can act as a candidate for leadership
/// in a fault-tolerant setup. Typically this would be a Service, but could
/// also be a communication line (e.g., socket connection).
///
/// CandidateState listens for leadership changes via notifications and provides
/// methods to mark the candidate as available or unavailable for election.
pub struct CandidateState {
    pub candidate_id: EntityId,
    pub is_leader: bool,

    notify_ch: (Sender<Notification>, Receiver<Notification>),

    ft: FT
}

/// State management for client services connected to qcore.
///
/// ServiceState handles:
/// - Periodic heartbeat writes to indicate the service is alive
/// - Optional fault-tolerant leadership election via CandidateState
/// - Automatic notification processing for leadership changes
///
/// # Example Usage
/// ```ignore
/// let mut service = ServiceState::new(
///     &mut store,
///     "my-service".to_string(),
///     true,  // fault_tolerant
///     1000   // heartbeat_interval_msecs
/// )?;
///
/// // In your main loop:
/// loop {
///     service.tick(&mut store)?;
///     // ... your service logic ...
/// }
/// ```
pub struct ServiceState {
    /// The machine this service is running on
    pub machine_id: String,

    /// The EntityId of the service instance
    pub service_id: EntityId,

    /// Whether this service participates in leader election
    pub fault_tolerant: bool,

    /// Candidate state for fault tolerance (only present if fault_tolerant is true)
    pub candidate_state: Option<CandidateState>,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_msecs: u64,
    last_heartbeat: std::time::Instant,

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
            ft,
        })
    }

    // Called back the main loop tick
    pub fn tick(&mut self, store: &mut StoreProxy) -> Result<()> {
        // Heartbeat update
        let now = std::time::Instant::now();
        if now.duration_since(self.last_heartbeat).as_millis() >= self.heartbeat_interval_msecs as u128 {
            self.write_heartbeat(store)?;
            self.last_heartbeat = now;
        }

        // Candidate tick - process notifications and update leadership status
        if let Some(ref mut candidate) = self.candidate_state {
            candidate.tick(store)?;
        }

        Ok(())
    }

    /// Returns true if this service is currently the leader (only meaningful if fault_tolerant is true)
    pub fn is_leader(&self) -> bool {
        self.candidate_state
            .as_ref()
            .map(|c| c.is_leader())
            .unwrap_or(false)
    }

    // Write heartbeat to the Service entity
    fn write_heartbeat(&mut self, store: &mut StoreProxy) -> Result<()> {
        let ft_heartbeat = self.ft.heartbeat.expect("Heartbeat field type should be defined");
        
        // Write a heartbeat value (Choice(0) is a common convention for "alive")
        store.write(
            self.service_id,
            &[ft_heartbeat],
            Value::Choice(0),
            Some(self.service_id), // writer_id
            None, // write_time
            None, // push_condition
            None, // adjust_behavior
        )?;

        Ok(())
    }
}

impl CandidateState {
    pub fn new(store: &mut StoreProxy, candidate_id: EntityId) -> Self {
        let ft = FT::new(store);
        let notify_ch = crossbeam::channel::unbounded();
        
        CandidateState {
            candidate_id,
            is_leader: false,
            notify_ch,
            ft,
        }
    }

    pub fn tick(&mut self, _store: &mut StoreProxy) -> Result<()> {
        // Check for notifications about leadership changes
        while let Some(notification) = self.notify_ch.1.try_recv().ok() {
            // The notification is for CurrentLeader field changes
            if let Some(Value::EntityReference(leader_ref)) = notification.current.value {
                let was_leader = self.is_leader;
                self.is_leader = leader_ref == Some(self.candidate_id);
                
                if was_leader != self.is_leader {
                    if self.is_leader {
                        log::info!("Candidate {:?} became leader", self.candidate_id);
                    } else {
                        log::info!("Candidate {:?} lost leadership", self.candidate_id);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn make_me_available(&mut self, store: &mut StoreProxy) -> Result<()> {
        // Writes the Candidate MakeMe field as "Available" (Choice(1)), allowing it to be elected as leader
        let ft_make_me = self.ft.make_me.expect("MakeMe field type should be defined");
        
        store.write(
            self.candidate_id,
            &[ft_make_me],
            Value::Choice(1), // 1 = Available
            Some(self.candidate_id), // writer_id
            None, // write_time
            Some(crate::PushCondition::Changes),
            None, // adjust_behavior
        )?;

        Ok(())
    }

    pub fn make_me_unavailable(&mut self, store: &mut StoreProxy) -> Result<()> {
        // Writes the Candidate MakeMe field as "Unavailable" (Choice(0)), preventing it from being elected as leader
        let ft_make_me = self.ft.make_me.expect("MakeMe field type should be defined");
        
        store.write(
            self.candidate_id,
            &[ft_make_me],
            Value::Choice(0), // 0 = Unavailable
            Some(self.candidate_id), // writer_id
            None, // write_time
            Some(crate::PushCondition::Changes),
            None, // adjust_behavior
        )?;

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

