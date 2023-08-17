from datetime import datetime
from logging import Logger
import uuid
from collections import Counter

from lib.kafka_connect.kafka_connectors import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger
        

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
 
            self._logger.info(f'{datetime.utcnow()}: Message received')

            try:
                order = msg['payload']
            except:
                break 

            self._logger.info(f'{datetime.utcnow()}: Insert into cdm.user_product_counters')

            l_order_product = order['l_order_product']
            h_user = order['h_user']
            s_product_names = order['s_product_names']
            h_product = order['h_product']

            count_product_order = dict(Counter([h['h_product_pk'] for h in l_order_product]))
            user_id = uuid.uuid3(uuid.NAMESPACE_OID, str(h_user['user_id']))

            for product in count_product_order.items():
                for h_name in s_product_names:
                    if h_name['h_product_pk'] == product[0]:
                        for p_name in h_product:
                            if p_name['h_product_pk'] == h_name['h_product_pk']:
                                self._cdm_repository.cdm_insert(user_id,
                                                        uuid.uuid3(uuid.NAMESPACE_OID, str(p_name['h_product_pk'])),
                                                        h_name['name'],
                                                        product[1],
                                                        'user_product_counters',
                                                        ['user_id', 'product_id', 'product_name', 'order_cnt'])
                                
            self._logger.info(f'{datetime.utcnow()}: Insert into cdm.user_category_counters')
            l_product_category = order['l_product_category']
            list_category_order = []
            for product in count_product_order.items():         
                for cat in l_product_category:
                    if product[0] == cat['h_product_pk']:
                        list_category_order.append(cat['h_category_pk'])
            
            count_category_order = dict(Counter(list_category_order))
            h_category = order['h_category']

            for cat in count_category_order.items():
                for cat_name in h_category:
                    if cat[0] == cat_name['h_category_pk']:
                        self._cdm_repository.cdm_insert(user_id,
                                                        uuid.uuid3(uuid.NAMESPACE_OID, str(cat_name['h_category_pk'])),
                                                        cat_name['category_id'],
                                                        product[1],
                                                        'user_category_counters',
                                                        ['user_id', 'category_id', 'category_name', 'order_cnt'])


        self._logger.info(f"{datetime.utcnow()}: FINISH")
