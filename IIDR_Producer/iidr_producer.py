from kafka import KafkaProducer
import json
import sys
producer_topic = 'iidr_trans'
valid_table_key = ['s_doc_quote', 's_quote_tntx']

def produceMsg(file_pth, tbl_key):
    print("Inside Produce:")
    hdr = [('Table.Key', tbl_key.encode())]
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("file path: ", file_pth)
    file = open(file_pth, "r")
    print(file)
    count = 0
    for line in file:
        count+=1
        msg_val = line.strip("\n")
        print("msg:", msg_val)
        producer.send(producer_topic, value=msg_val.encode(), headers=hdr)
        producer.flush()
    print("{0} messages sent successfully:".format(count))

    # preparing msg
    #msg = {"ROW_ID": "M-KYK0ZWH", "CREATED": "2020-04-12 17:37:19.0", "ODS_LOAD_DATETIME": "2020-04-12 17:44:59.856", "CREATED_BY": "S-2KG5E-319","PAR_ROW_ID": "M-KYK0ZWH", "A_ENTTYP": "UP", "ODS_PROPERTY_ID": "M-C3W5OOE"}
    # msg['ROW_ID'] = msg['ROW_ID'] + '-' + str(i)
    # msg_val = json.dumps(msg)
    # msg_val['ROW_ID']
    # disc = json.loads(msg_val)

    # print("msg_val:", msg_val)

def getFileName():
    print("Inside Filename:")
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        print("Note: Please enter Filepath Parameter!!")
        exit()

    slsh_pos = filepath.rindex("\\")
    filename = filepath[slsh_pos+1:]
    table_key = filename[:filename.rindex('.')]
    if table_key in valid_table_key:
        print(table_key)
    else:
        print("Enter valid file name: ")
        exit()
    print(filepath)
    return(table_key, filepath)


if __name__ == "__main__":
    print("Inside Main: ")
    table_key, file_path = getFileName()
    print("Inside Main 2:")
    print(table_key, file_path)
    produceMsg(file_path, table_key)

