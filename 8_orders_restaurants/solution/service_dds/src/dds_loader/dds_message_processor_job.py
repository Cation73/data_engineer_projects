from datetime import datetime
from logging import Logger
import uuid
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            self._logger.info(f'{datetime.utcnow()}: Message received')
            try:
                order = msg['payload']
            except:
                break
            load_src = 'dds-layer-orders-kafka'

            self._logger.info(f'{datetime.utcnow()}: Insert into hubs')
            self._logger.info(f'{datetime.utcnow()}: Insert into dds.h_restaurant')

            restaurant = order['restaurant']
            h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(restaurant['id']))
            h_restaurant_dt = datetime.utcnow()
            self._dds_repository.h_insert(h_restaurant_pk,
                                          restaurant['id'],
                                          h_restaurant_dt, 
                                          load_src,
                                          'h_restaurant',
                                          ['h_restaurant_pk', 'restaurant_id', 'load_dt', 'load_src'])

            self._logger.info(f'{datetime.utcnow()}: Insert into dds.h_user')

            user = order['user']
            h_user_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(user['id']))
            h_user_dt = datetime.utcnow()
            self._dds_repository.h_insert(h_user_pk,
                                          user['id'],
                                          h_user_dt, 
                                          load_src,
                                          'h_user',
                                          ['h_user_pk', 'user_id', 'load_dt', 'load_src'])
            
            h_product_category_dt = datetime.utcnow()
            h_product = []
            h_category = []
            for idx, product in enumerate(order['products']):
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.h_product - {idx+1} iterations')
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(product['id']))
                self._dds_repository.h_insert(h_product_pk,
                                          product['id'],
                                          h_product_category_dt, 
                                          load_src,
                                          'h_product',
                                          ['h_product_pk', 'product_id', 'load_dt', 'load_src'])
                
                h_product.append({'h_product_pk': str(h_product_pk), 
                                            'product_id': product['id'],
                                            'load_dt': h_product_category_dt, 
                                            'load_src': load_src})
                
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.h_category - {idx+1} iterations')
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(product['category']))
                self._dds_repository.h_insert(h_category_pk,
                                          product['category'],
                                          h_product_category_dt, 
                                          load_src,
                                          'h_category',
                                          ['h_category_pk', 'category_name', 'load_dt', 'load_src'])

                h_category.append({'h_category_pk': str(h_category_pk), 
                                            'category_id': product['category'],
                                            'load_dt': h_product_category_dt, 
                                            'load_src': load_src})

            self._logger.info(f'{datetime.utcnow()}: Insert into dds.h_order')

            h_order_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(order['id']))
            h_order_dt = datetime.utcnow()
            self._dds_repository.h_order_insert(h_order_pk,
                                          order['id'],
                                          order['date'],
                                          h_order_dt,
                                          load_src)
            
            self._logger.info(f'{datetime.utcnow()}: Insert into links')

            self._logger.info(f'{datetime.utcnow()}: Insert into dds.l_order_user')

            hk_order_user_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_order_pk) + str(h_user_pk))
            l_order_user_dt = datetime.utcnow()
            self._dds_repository.l_insert(hk_order_user_pk,
                                          h_order_pk,
                                          h_user_pk,
                                          l_order_user_dt,
                                          load_src,
                                          'l_order_user',
                                          ['hk_order_user_pk', 'h_order_pk', 'h_user_pk', 'load_dt', 'load_src'])
            l_product_category = []
            l_product_restaurant = []
            l_order_product = []
            l_product_category_dt = datetime.utcnow()

            for idx, product in enumerate(order['products']):
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.l_product_category - {idx+1} iterations')

                h_product_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(product['id']))
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(product['category']))
                hk_product_category_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_product_pk) + str(h_category_pk))
                self._dds_repository.l_insert(hk_product_category_pk,
                                          h_product_pk,
                                          h_category_pk,
                                          l_product_category_dt, 
                                          load_src,
                                          'l_product_category',
                                          ['hk_product_category_pk', 'h_product_pk', 'h_category_pk', 'load_dt', 'load_src'])
                
                l_product_category.append({'hk_product_category_pk': str(hk_product_category_pk), 
                                            'h_product_pk': str(h_product_pk),
                                            'h_category_pk': str(h_category_pk),
                                            'load_dt': l_product_category_dt, 
                                            'load_src': load_src})
                
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.l_product_restaurant - {idx+1} iterations')
                hk_product_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_product_pk) + str(h_restaurant_pk))
                self._dds_repository.l_insert(hk_product_restaurant_pk,
                                          h_product_pk,
                                          h_restaurant_pk,
                                          l_product_category_dt, 
                                          load_src,
                                          'l_product_restaurant',
                                          ['hk_product_restaurant_pk', 'h_product_pk', 'h_restaurant_pk', 'load_dt', 'load_src'])
                
                l_product_restaurant.append({'hk_product_restaurant_pk': str(hk_product_restaurant_pk), 
                                            'h_product_pk': str(h_product_pk),
                                            'h_restaurant_pk': str(h_restaurant_pk),
                                            'load_dt': l_product_category_dt, 
                                            'load_src': load_src})
                
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.l_order_product - {idx+1} iterations')
                hk_order_product_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_order_pk) + str(h_product_pk))
                self._dds_repository.l_insert(hk_order_product_pk,
                                          h_order_pk,
                                          h_product_pk,
                                          l_product_category_dt, 
                                          load_src,
                                          'l_order_product',
                                          ['hk_order_product_pk', 'h_order_pk', 'h_product_pk', 'load_dt', 'load_src'])

                l_order_product.append({'hk_order_product_pk': str(hk_order_product_pk), 
                                            'h_order_pk': str(h_order_pk),
                                            'h_product_pk': str(h_product_pk),
                                            'load_dt': l_product_category_dt, 
                                            'load_src': load_src})

            self._logger.info(f'{datetime.utcnow()}: Insert into satelites')

            self._logger.info(f'{datetime.utcnow()}: Insert into dds.s_user_names')
            hk_user_names_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_user_pk) + str(user['name']))
            s_user_names_dt = datetime.utcnow()
            self._dds_repository.s_user_insert(hk_user_names_pk, 
                                               h_user_pk,
                                               user['name'],
                                               #user['login'], 
                                               'userlogin',
                                               s_user_names_dt, 
                                               load_src,
                                               's_user_names',
                                                ['hk_user_names_pk', 'h_user_pk', 'username', 'userlogin', 'load_dt', 'load_src'])

            self._logger.info(f'{datetime.utcnow()}: Insert into dds.s_order_cost')
            hk_order_cost_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_order_pk) + str(order['cost']))
            s_order_cost_dt = datetime.utcnow()
            self._dds_repository.s_cost_insert(hk_order_cost_pk, 
                                               h_order_pk,
                                               order['cost'],
                                               order['payment'],
                                               s_order_cost_dt, 
                                               load_src,
                                               's_order_cost',
                                                ['hk_order_cost_pk', 'h_order_pk', 'cost', 'payment', 'load_dt', 'load_src'])
            
            self._logger.info(f'{datetime.utcnow()}: Insert into dds.s_order_status')
            hk_order_status_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_order_pk) + str(order['status']))
            s_order_status_dt = datetime.utcnow()
            self._dds_repository.s_res_product_status_insert(hk_order_status_pk, 
                                               h_order_pk,
                                               order['status'],
                                               s_order_status_dt, 
                                               load_src,
                                               's_order_status',
                                                ['hk_order_status_pk', 'h_order_pk', 'status', 'load_dt', 'load_src'])
            
            self._logger.info(f'{datetime.utcnow()}: Insert into dds.s_restaurant_names')
            hk_restaurant_names_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_restaurant_pk) + str(restaurant['name']))
            s_restaurant_names_dt = datetime.utcnow()
            self._dds_repository.s_res_product_status_insert(hk_restaurant_names_pk, 
                                               h_restaurant_pk,
                                               restaurant['name'],
                                               s_restaurant_names_dt, 
                                               load_src,
                                               's_restaurant_names',
                                                ['hk_restaurant_names_pk', 'h_restaurant_pk', 'name', 'load_dt', 'load_src'])
            
            s_product_names = []
            s_product_names_dt = datetime.utcnow()
            for idx, product in enumerate(order['products']):
                self._logger.info(f'{datetime.utcnow()}: Insert into dds.s_product_names - {idx+1} iterations')

                h_product_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(product['id']))
                hk_product_names_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(h_product_pk) + str(h_category_pk))
                self._dds_repository.s_res_product_status_insert(hk_product_names_pk,
                                          h_product_pk,
                                          product['name'],
                                          s_product_names_dt, 
                                          load_src,
                                          's_product_names',
                                          ['hk_product_names_pk', 'h_product_pk', 'name', 'load_dt', 'load_src'])
                
                s_product_names.append({'hk_product_names_pk': str(hk_product_names_pk), 
                                            'h_product_pk': str(h_product_pk),
                                            'name': product['name'],
                                            'load_dt': s_product_names_dt, 
                                            'load_src': load_src})
                

            cdm_msg  = {'object_id' : msg['object_id'],
                        'object_type': 'order',
                        'payload': {
                            'h_restaurant': {'h_restaurant_pk': str(h_restaurant_pk), 
                                            'restaurant_id': restaurant['id'],
                                            'load_dt': h_restaurant_dt,
                                            'load_src': load_src},
                            'h_user': {'h_user_pk': str(h_user_pk),
                                    'user_id': user['id'],
                                    'load_dt': h_user_dt,
                                    'load_src': load_src},
                            
                            'h_product': h_product,
                            'h_category': h_category,
                            'h_order': {'h_order_pk': str(h_order_pk),
                                    'order_id': order['id'],
                                    'order_dt': order['date'],
                                    'load_dt': h_order_dt,
                                    'load_src': load_src},
                            'l_order_user': {'hk_order_user_pk': str(hk_order_user_pk),
                                             'h_order_pk': str(h_order_pk),
                                             'h_user_pk': str(h_user_pk),
                                             'load_dt': l_order_user_dt,
                                             'load_src': load_src},
                            'l_order_product': l_order_product,
                            'l_product_category': l_product_category,
                            'l_product_restaurant': l_product_restaurant,
                            's_user_names': {'hk_user_names_pk': str(hk_user_names_pk),
                                             'h_user_pk': str(h_user_pk),
                                             'username': user['name'],
                                             'userlogin': 'userlogin', #user['login'],
                                             'load_dt': s_user_names_dt,
                                             'load_src': load_src},
                            's_order_cost': {'hk_order_cost_pk': str(hk_order_cost_pk),
                                             'h_order_pk': str(h_order_pk),
                                             'cost': order['cost'],
                                             'payment': order['payment'],
                                             'load_dt': s_order_cost_dt,
                                             'load_src': load_src},
                            's_order_status': {'hk_order_cost_pk': str(hk_order_status_pk),
                                               'h_order_pk': str(h_order_pk),
                                               'status': order['status'],
                                               'load_dt': s_order_status_dt,
                                               'load_src': load_src},
                            's_restaurant_names': {'hk_restaurant_names_pk': str(hk_restaurant_names_pk),
                                                   'h_restaurant_pk': str(h_restaurant_pk), 
                                                   'name': restaurant['name'],
                                                   'load_dt':s_restaurant_names_dt,
                                                   'load_src': load_src},
                            's_product_names': s_product_names
                                    }
                        }

            self._producer.produce(cdm_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")
            
        self._logger.info(f"{datetime.utcnow()}: FINISH")

