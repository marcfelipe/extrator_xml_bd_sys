import fdb
from Connection import conectar_db
from datetime import datetime
import time


from functions import generate_list_to_export, parse_xml_db, decompress_db_content


db_path = input('db_path:\n')
if db_path =='':
  db_path = 'c:/syspdv/syspdv_srv.fdb'
  

print('Db path set to:', db_path)
server_name = input('server_name:\n')
if server_name =='':
  server_name = 'localhost'

print('Server_name: ', server_name)
output_folder = input('output_folder:\n')
if output_folder =='':
  output_folder = 'G:/exported_xmls/'

print('=====Data deve possuir o formato: dd.mm.aaaa=====\n')
initial_date = input('Digite a data inicial para exportar:\n')
final_date = input('Digite a data Final para exportar:\n')

#caixa = input('type checkout number that will be reprocessed!\n')
  
debugging = False
#initial_date = '01.06.2022'
#final_date = '30.06.2022'




list_data = generate_list_to_export(server_name, db_path, initial_date, final_date)
print(str(len(list_data)), 'Notas serão exportadas')


count_items_processed = 0
for line_data in list_data:
  trnseq = line_data[0]
  trndat = line_data[1]  
  #trndat = datetime.strftime(trndat, '%d.%m.%Y')
  cxanum = line_data[2]
  db_content = line_data[3]
#  print('processing data from transaction:',trnseq, trndat, cxanum)
  #print(db_content)
  doc_tmp = parse_xml_db(db_content)
  #print(doc_tmp)
  if debugging == True:
    with open('debug_xml.xml','wb') as debug_file:
      content = decompress_db_content(db_content)
      debug_file.write(content)

  try:
    if "nfeProc" in doc_tmp.keys():
      chave_nfe = doc_tmp["nfeProc"]["NFe"]["infNFe"]["@Id"][3:47]
    elif "procEventoNFe" in doc_tmp.keys():
      chave_nfe = doc_tmp["procEventoNFe"]["NFe"]["infNFe"]["@Id"][3:47]
    else:
      chave_nfe = doc_tmp["NFe"]["infNFe"]["@Id"][3:47]
  except Exception as e:
    print('Erro ao obter chave nfe para trnseq:', trnseq, 'erro:', str(e))
    with open('debug_xml.xml','wb') as debug_file:
      content = decompress_db_content(db_content)
      debug_file.write(content)
    continue
  #print('Exportando nfce:', chave_nfe)
  xml_content = decompress_db_content(db_content)
  with open(output_folder + chave_nfe + '.xml', 'wb') as xml_file:
    xml_file.write(xml_content)
    result_export = True

  count_items_processed += 1
#  if result_export:
#    print(f'{count_items_processed} processados')
      



print(str(count_items_processed), 'Notas foram exportadas')
print('=====processo finalizado=====')

