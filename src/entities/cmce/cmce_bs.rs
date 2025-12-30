use crate::config::stack_config::SharedConfig;
use crate::common::messagerouter::MessageQueue;
use crate::entities::cmce::enums::cmce_pdu_type_ul::CmcePduTypeUl;
use crate::saps::sapmsg::{SapMsg, SapMsgInner};
use crate::common::tetra_entities::TetraEntity;
use crate::entities::TetraEntityTrait;
use crate::common::tetra_common::Sap;
use crate::unimplemented_log;

use super::subentities::cc::CcSubentity;
use super::subentities::sds::SdsSubentity;
use super::subentities::ss::SsSubentity;

pub struct CmceBs {
    config: SharedConfig,
    
    sds: SdsSubentity,
    cc: CcSubentity,
    ss: SsSubentity,
}

impl CmceBs {
    pub fn new(config: SharedConfig) -> Self {
        Self { 
            config,
            sds: SdsSubentity::new(),
            cc: CcSubentity::new(),
            ss: SsSubentity::new(),
         }
    }

    pub fn rx_unitdata_ind(&mut self, _queue: &mut MessageQueue, mut message: SapMsg) {
        tracing::trace!("rx_unitdata_ind");
        
        // Handle the incoming unit data indication
        let SapMsgInner::LcmcMleUnitdataInd(prim) = &mut message.msg else { panic!(); };
        let Some(bits) = prim.sdu.peek_bits(5) else {
            tracing::warn!("insufficient bits: {}", prim.sdu.dump_bin());
            return;
        };
        let Ok(pdu_type) = CmcePduTypeUl::try_from(bits) else {
            tracing::warn!("invalid pdu type: {} in {}", bits, prim.sdu.dump_bin());
            return;
        };

        tracing::debug!("CmceBs rx_unitdata_ind for pdu_type {:?}", pdu_type);

        unimplemented_log!("{:?}", pdu_type);
    }
}

impl TetraEntityTrait for CmceBs {

    fn entity(&self) -> TetraEntity {
        TetraEntity::Cmce
    }

    fn set_config(&mut self, config: SharedConfig) {
        self.config = config;
    }

    fn rx_prim(&mut self, queue: &mut MessageQueue, message: SapMsg) {
        
        tracing::debug!("rx_prim: {:?}", message);
        
        // There is only one SAP for CMCE
        assert!(message.sap == Sap::LcmcSap);

        match message.msg {
            SapMsgInner::LcmcMleUnitdataInd(_) => {
                self.rx_unitdata_ind(queue, message);
            }
            _ => {
                panic!();
            }
        }
    }
}
