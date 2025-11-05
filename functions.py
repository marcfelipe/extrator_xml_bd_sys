from Connection import conectar_db
import zlib, xmltodict
from datetime import datetime


def generate_list_to_process(server_name, db_path, initial_date, final_date):
  sql_generate_list = """select tx.trnseq,tx.trndat,tx.cxanum,trnxml from transacao_xmlnota tx 
  left join transacao t on (tx.trnseq=t.trnseq and tx.cxanum=t.cxanum and tx.trndat=t.trndat) 
  where t.trnimpcod='99' and trntip='1'
  and tx.trndat>=? and tx.trndat<=? 
  and trnsta in ('PE','ER','RJ')"""

  con = conectar_db(server_name, db_path)
  cursor = con.cursor()
  cursor.execute(sql_generate_list,[initial_date, final_date])
  result_set = cursor.fetchall()
  con.close()
  return result_set

def generate_list_to_export(server_name, db_path, initial_date, final_date):
  sql_generate_list = """select tx.trnseq,tx.trndat,tx.cxanum,trnxml from transacao_xmlnota tx 
  left join transacao t on (tx.trnseq=t.trnseq and tx.cxanum=t.cxanum and tx.trndat=t.trndat) 
  where t.trnimpcod='99' and trntip='1'
  and tx.trndat>=? and tx.trndat<=? 
  and trnsta not in ('PE','ER','RJ')"""

  con = conectar_db(server_name, db_path)
  cursor = con.cursor()
  cursor.execute(sql_generate_list,[initial_date, final_date])
  result_set = cursor.fetchall()
  con.close()
  return result_set


source_db = 'c:/syspdv/syspdv_srv_E_n_LIMA_070422.fdb'

# test = generate_list_to_process('localhost', source_db, '01.04.2022', '06.04.2022')

# if test is not None:
#   print(test[0])
#   print(test[1])
#   print('length from test is:', str(len(test)))
#for line in test:
#  print('sequencial:', line[0], 'cxanum:', line[2])

def decompress_db_content(db_content):
  xml_content = zlib.decompress(db_content)
  return xml_content

def parse_xml_db(db_content):
  xml_content = zlib.decompress(db_content)
  doc = xmltodict.parse(xml_content)
  return doc

def xml_compress_for_db(xml_content):
  out_doc = xmltodict.unparse(xml_content, pretty=False)
  print('Realizando compressão')
  new_db_content = zlib.compress(out_doc.encode('utf-8'))
  return new_db_content

def update_xml_in_db(server_name,db_path,trnseq,cxanum,trndat,db_content):
  con = conectar_db(server_name, db_path)
  cursor = con.cursor()
  sql_update_xml_bd = """update transacao_xmlnota set trnxml = ? 
  where trnseq=? and cxanum=? and trndat=?"""
  cursor.execute(sql_update_xml_bd,[db_content, trnseq, cxanum, trndat])
  con.commit()
  con.close()
  return True

