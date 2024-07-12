import fdb
from Connection import conectar_db
from datetime import datetime
import time


from functions import generate_list_to_process, parse_xml_db, xml_compress_for_db, update_xml_in_db


db_path = input('db_path:\n')
if db_path =='':
  db_path = 'c:/syspdv/syspdv_srv.fdb'
server_name = input('server_name:\n')
if server_name =='':
  server_name = 'localhost'

print('=====the date has to be in dd.mm.aaaa format=====\n')
initial_date = input('Type the initial date for the interval to modify:\n')
final_date = input('Type the final date for the interval to modify:\n')

#caixa = input('type checkout number that will be reprocessed!\n')
  

initial_date = '14.01.2023'
final_date = '14.01.2023'

#Decision what type of service will be fired
att_indpres = False
att_csosn = False
change_cfop = False
change_cbenef = True   #this function is not working.


list_data = generate_list_to_process(server_name, db_path, initial_date, final_date)
print(str(len(list_data)), 'items will be processed.')

def update_indPres(doc_xml):
  doc_xml["NFe"]["infNFe"]["ide"]["indPres"] = '1'
  return doc_xml

def update_csosn(doc_xml):
  det = doc_xml["NFe"]["infNFe"]["det"]
  if isinstance(det, list):
    for content in det:
      cfop = str(content['prod']['CFOP']).strip()      
      icms = content['imposto']['ICMS']
      serie = doc_xml["NFe"]["infNFe"]["ide"]["serie"]
      numero_nota = doc_xml["NFe"]["infNFe"]["ide"]["nNF"]
      #if numero_nota == 160783:
      #  print(content)
      #  time.sleep(5)
      new_key = 'ICMSSN500'
      old_key = 'ICMSSN'
      if cfop == '5405' and old_key in icms:
        print('encontrado ICMSSN e cfop 5405 EM ', serie, numero_nota)
        content['imposto']['ICMS']['ICMSSN500']=content['imposto']['ICMS'].pop('ICMSSN')
        content['imposto']['ICMS']['ICMSSN500']['CSOSN']='500'
      elif cfop == '5102' and old_key in icms:
        print('encontrado ICMSSN e cfop 5405 EM ', serie, numero_nota)
        content['imposto']['ICMS']['ICMSSN102']=content['imposto']['ICMS'].pop('ICMSSN')
        content['imposto']['ICMS']['ICMSSN102']['CSOSN']='102'
      else:
        print('nothing to process:', str(content['imposto']['ICMS']))
  else:
    cfop = str(det['prod']['CFOP']).strip()      
    icms = det['imposto']['ICMS']
    serie = doc_xml["NFe"]["infNFe"]["ide"]["serie"]
    numero_nota = doc_xml["NFe"]["infNFe"]["ide"]["nNF"]
    #if numero_nota == 160783:
    #  print(det)
    #  time.sleep(5)
    new_key = 'ICMSSN500'
    old_key = 'ICMSSN'
    if cfop == '5405' and old_key in icms:
      print('encontrado ICMSSN e cfop 5405 EM ', serie, numero_nota)
      det['imposto']['ICMS']['ICMSSN500']=det['imposto']['ICMS'].pop('ICMSSN')
      det['imposto']['ICMS']['ICMSSN500']['CSOSN']='500'
    elif cfop == '5102' and old_key in icms:
      print('encontrado ICMSSN e cfop 5405 EM ', serie, numero_nota)
      det['imposto']['ICMS']['ICMSSN102']=det['imposto']['ICMS'].pop('ICMSSN')
      det['imposto']['ICMS']['ICMSSN102']['CSOSN']='102'
    else:
      print('nothing to process:', str(det['imposto']['ICMS']))


    

  return doc_xml

def update_cfop_54030(doc_xml):
  det = doc_xml["NFe"]["infNFe"]["det"]
  if isinstance(det, list):
    for content in det:
      cfop = str(content['prod']['CFOP']).strip()      
      icms = content['imposto']['ICMS']
      serie = doc_xml["NFe"]["infNFe"]["ide"]["serie"]
      numero_nota = doc_xml["NFe"]["infNFe"]["ide"]["nNF"]
      if cfop == '5403':
        print('encontrado cfop 5403 trocando para 5405 ', serie, numero_nota)        
        content['prod']['CFOP']='5405'
  else:
    cfop = str(det['prod']['CFOP']).strip()
    if cfop == '5403':
      det['prod']['CFOP']='5405'


  return doc_xml


def update_cbenef(doc_xml):
  det = doc_xml["NFe"]["infNFe"]["det"]
  if isinstance(det, list):
    for content in det:
      if 'cBenef' in content['prod'].keys():
        #content['prod'].keys().pop("cBenef") #return ordered dict has no pop method
        del content['prod']['cBenef']

  return doc_xml


count_items_processed = 0
for line_data in list_data:
  trnseq = line_data[0]
  trndat = line_data[1]  
  #trndat = datetime.strftime(trndat, '%d.%m.%Y')
  cxanum = line_data[2]
  db_content = line_data[3]
  print('processing data from transaction:',trnseq, trndat, cxanum)
  #print(db_content)
  doc_tmp = parse_xml_db(db_content)
  if att_indpres:
    doc_tmp = update_indPres(doc_tmp)
  if att_csosn:
    doc_tmp = update_csosn(doc_tmp)
  if change_cfop:
    doc_tmp = update_cfop_54030(doc_tmp)
  if change_cbenef:
    doc_tmp = update_cbenef(doc_tmp)

  #add new functions here.
  # 
  # 


  new_db_content = xml_compress_for_db(doc_tmp)
  result_update = update_xml_in_db(server_name, db_path, trnseq, cxanum, trndat, new_db_content)
  count_items_processed += 1
  if result_update:
    print('Updates processed successfully')
      



print(str(count_items_processed), 'items were processed')
print('=====process finished=====')

