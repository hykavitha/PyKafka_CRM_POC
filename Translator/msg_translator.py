import json
import time
from kafka import KafkaConsumer, KafkaProducer


class msgTranlator:
    tran_consumer_topic = 'iidr_trans'
    dict_table_flowtyp_map = {'s_doc_quote': 'rest_enrich', 's_quote_tntx':'pid_enrich', 's_oppty':'no_enrich', 'xyz':'rest_pid_enrich'}
    dict_flowtyp_tpc_map = {'rest_enrich':'trans_rest_enrich', 'pid_enrich': 'trans_pid_enrich', 'no_enrich': 'trans_no_enrich', 'rest_pid_enrich':'trans_rest_pid_enrich'}

    def tran_consume_msg(self):
        print("Inside Consume")
        trans_consumer = KafkaConsumer(self.tran_consumer_topic, bootstrap_servers='localhost:9092')
        print("after consumer")
        for tran_msg in trans_consumer:
            print("for")
            print(tran_msg)
            trans_hdr = self.tran_prep_header(tran_msg)
            prodcr_topic = self.dict_flowtyp_tpc_map.get((trans_hdr['Flow.Type']).decode(), 'errortopic')
            print(prodcr_topic)
            self.tran_produce_msg(tran_msg.value, trans_hdr, prodcr_topic)
        return ''


    def get_traceId(self, traceValue):
        print("Inside traceId:")
        row_id = traceValue['ROW_ID']
        print(row_id)
        ts = time.time()
        print(ts)
        tr_id = "ROWID_" + row_id + "_" + str(ts) + "_MsgTranslator"
        return (tr_id)

    def tran_prep_header(self, msg):
        json_msg = json.loads(msg.value)
        trace_id = self.get_traceId(json_msg).encode()
        print(trace_id)

        header = msg.headers
        dict_hdr = dict(header)
        table_key = (dict_hdr['Table.Key']).decode()
        flow_type = (self.dict_table_flowtyp_map[table_key]).encode()
        add_hdr = {'TRACE_ID': trace_id, 'Flow.Type':flow_type}
        tran_hdr = {**dict_hdr, **add_hdr}
        return(tran_hdr)

    def tran_produce_msg(self, msg_val, msg_hdr, tpc_nm):
        print("Inside Produce")
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        # Header processing
        print(type(msg_hdr))
        msg_hdr_lst = list(msg_hdr.items())
        print("Trans hdr lst:", msg_hdr_lst)
        producer.send(tpc_nm, value=msg_val, headers=msg_hdr_lst)
        producer.flush()






