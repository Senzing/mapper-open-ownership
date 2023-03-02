#! /usr/bin/env python3

import sys
import os
import argparse
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import hashlib
import gzip
import io


#=========================
class mapper():

    def __init__(self):

        self.stat_pack = {}
        self.conversions = {}
        self.conversions['ADDR_TYPE'] = {}
        self.conversions['ADDR_TYPE']['REGISTERED'] = 'BUSINESS'
        self.conversions['ID_TYPE'] = {}
        self.conversions['ID_TYPE']['DK-CVR'] = ['NATIONAL_ID', 'DNK']
        self.conversions['ID_TYPE']['GB-COH'] = ['NATIONAL_ID', 'GBR']
        self.conversions['ID_TYPE']['SK-ORSR'] = ['NATIONAL_ID', 'SVK']
        self.conversions['ID_TYPE']['UA-EDR'] = ['NATIONAL_ID', 'UKR']
        self.conversions['ID_TYPE']['MISC-DENMARK CVR'] = ['NATIONAL_ID', 'DNK']
        self.conversions['ID_TYPE']['MISC-SLOVAKIA PSP REGISTER'] = ['NATIONAL_ID', 'SVK']

    def map(self, raw_data, input_row_num = None):
        json_data = {}

        statement_type = raw_data.get('statementType', 'none')
        entity_type = raw_data.get('entityType', 'none')

        json_data['DATA_SOURCE'] = 'OPEN-OWNER'
        if statement_type == 'entityStatement':
            json_data = self.map_entity(raw_data)
            json_data['RECORD_ID'] = raw_data['statementID']

        elif statement_type == 'personStatement':
            json_data = self.map_person(raw_data)
            json_data['RECORD_ID'] = raw_data['statementID']

        elif statement_type == 'ownershipOrControlStatement':
            entity_type = list(raw_data['interestedParty'].keys())[0]
            json_data = self.map_relationship(raw_data)
            json_data['RECORD_ID'] = raw_data['subject']['describedByEntityStatement']

        mapper.update_stat('!raw', 'statements', statement_type, entity_type)

        json_data = self.remove_empty_tags(json_data)

        return json_data

    def map_entity(self, raw_data):
        json_data = {}
        json_data['RECORD_TYPE'] = 'ORGANIZATION'

        json_data['PRIMARY_NAME_ORG'] = raw_data.get('name')

        json_data['ADDRESSES'] = []
        for addr_data in raw_data.get('addresses',[]):
            addr_full = addr_data.get('address')
            if addr_full:
                addr_country = addr_data.get('country','')
                raw_addr_type = addr_data.get('type','unknown-address').upper()
                mapper.update_stat('!raw', 'address_type', raw_addr_type)
                addr_type = self.conversions['ADDR_TYPE'].get(raw_addr_type, raw_addr_type)
                json_data['ADDRESSES'].append({'ADDR_TYPE': addr_type, 'ADDR_FULL': addr_full, 'ADDR_COUNTRY': addr_country})

        json_data['IDENTIFIERS'] = []
        json_data['LINKS'] = []
        #json_data['MISC'] = []
        for id_data in raw_data.get('identifiers',[]):
            id_value = id_data.get('id')
            id_uri = id_data.get('uri')
            if id_value or id_uri:
                scheme = id_data.get('scheme','')
                schemeName = id_data.get('schemeName','')
                if id_uri:
                    json_data['LINKS'].append({schemeName: id_uri})
                elif scheme and id_value:
                    mapper.update_stat('!raw', 'id-entity', scheme, value=id_value)
                    senzing_attr, country = self.conversions['ID_TYPE'].get(scheme, ('OTHER_ID', ''))
                    if senzing_attr == 'NATIONAL_ID':
                        mapped_data = {'NATIONAL_ID_NUMBER': id_value, 'NATIONAL_ID_TYPE': scheme, 'NATIONAL_ID_COUNTRY': country}
                    elif senzing_attr == 'OTHER_ID':
                        mapped_data = {'OTHER_ID_NUMBER': id_value, 'OTHER_ID_TYPE': scheme, 'OTHER_ID_COUNTRY': country}
                    else:
                        mapped_data = {senzing_attr: id_value}
                    json_data['IDENTIFIERS'].append(mapped_data)
                else:
                    #json_data['MISC'].append({schemeName: id_value})
                    mapper.update_stat('!raw', 'id-entity', 'schemeNameOnly', schemeName, value=id_value)

        json_data['RELATIONSHIP_LIST'] = [{'REL_ANCHOR_DOMAIN': 'OOC', 'REL_ANCHOR_KEY': raw_data['statementID']}]

        return json_data

    def map_person(self, raw_data):
        json_data = {}
        json_data['RECORD_TYPE'] = 'PERSON'

        json_data['OTHER_NAMES'] = []
        for name_data in raw_data.get('names',[]):
            name_value = name_data.get('fullName')
            if name_value:
                raw_name_type = name_data.get('type')
                mapper.update_stat('!raw', 'address_type', raw_name_type)
                if 'PRIMARY_NAME_FULL' not in json_data:
                    json_data['PRIMARY_NAME_FULL'] = name_value
                else:
                    json_data['OTHER_NAMES'].append({'NAME_TYPE': raw_name_type, 'NAME_FULL': name_value})

        dob = raw_data.get('birthDate',[])
        if dob:
            json_data['DATE_OF_BIRTH'] = dob

        json_data['ADDRESSES'] = []
        unspecified_addr_type = 'PRIMARY'
        for addr_data in raw_data.get('addresses',[]):
            addr_full = addr_data.get('address')
            if addr_full:
                addr_country = addr_data.get('country','')
                raw_addr_type = addr_data.get('type',unspecified_addr_type).upper()
                mapper.update_stat('!raw', 'address_type', raw_addr_type)
                addr_type = self.conversions['ADDR_TYPE'].get(raw_addr_type, raw_addr_type)
                json_data['ADDRESSES'].append({'ADDR_TYPE': addr_type, 'ADDR_FULL': addr_full, 'ADDR_COUNTRY': addr_country})
                unspecified_addr_type = 'OTHER'

        json_data['IDENTIFIERS'] = []
        json_data['LINKS'] = []
        #json_data['MISC'] = []
        for id_data in raw_data.get('identifiers',[]):
            id_value = id_data.get('id')
            id_uri = id_data.get('uri')
            if id_value or id_uri:
                scheme = id_data.get('scheme','')
                schemeName = id_data.get('schemeName','')
                if id_uri:
                    json_data['LINKS'].append({schemeName: id_uri})
                elif scheme and id_value:
                    mapper.update_stat('!raw', 'id-person', scheme, value=id_value)
                    senzing_attr, country = self.conversions['ID_TYPE'].get(scheme, ('OTHER_ID', ''))
                    if senzing_attr == 'NATIONAL_ID':
                        mapped_data = {'NATIONAL_ID_NUMBER': id_value, 'NATIONAL_ID_TYPE': scheme, 'NATIONAL_ID_COUNTRY': country}
                    elif senzing_attr == 'OTHER_ID':
                        mapped_data = {'OTHER_ID_NUMBER': id_value, 'OTHER_ID_TYPE': scheme, 'OTHER_ID_COUNTRY': country}
                    else:
                        mapped_data = {senzing_attr: id_value}
                    json_data['IDENTIFIERS'].append(mapped_data)
                else:
                    #json_data['MISC'].append({schemeName: id_value})
                    mapper.update_stat('!raw', 'id-person', 'schemeNameOnly', schemeName, value=id_value)

        json_data['RELATIONSHIP_LIST'] = [{'REL_ANCHOR_DOMAIN': 'OOC', 'REL_ANCHOR_KEY': raw_data['statementID']}]

        return json_data

    def map_relationship(self, raw_data):

        if raw_data['interestedParty'].get('describedByPersonStatement'):
            rel_pointer_key = raw_data['interestedParty'].get('describedByPersonStatement')
        elif raw_data['interestedParty'].get('describedByEntityStatement'):
            rel_pointer_key = raw_data['interestedParty'].get('describedByEntityStatement')
        else:
            rel_pointer_key = 'unknown'

        interest_types = []
        for interest_data in raw_data.get('interests', []):
            if interest_data.get('type'):
                interest_types.append(interest_data.get('type').replace('-', '_'))
        rel_pointer_role = '|'.join(set(interest_types)) if interest_types else 'unknown'

        return {'RELATIONSHIP_LIST': [{'REL_POINTER_DOMAIN': 'OOC', 'REL_POINTER_KEY': rel_pointer_key, 'REL_POINTER_ROLE': rel_pointer_role}]}

    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    def format_date(self, raw_date):
        try:
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except:
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    def update_stat(self, *args, **kwargs):

        if len(args) > 0 and args[0] not in self.stat_pack:
            self.stat_pack[args[0]] = {}
        if len(args) > 1 and args[1] not in self.stat_pack[args[0]]:
            self.stat_pack[args[0]][args[1]] = {}
        if len(args) > 2 and args[2] not in self.stat_pack[args[0]][args[1]]:
            self.stat_pack[args[0]][args[1]][args[2]] = {}
        if len(args) > 3 and args[3] not in self.stat_pack[args[0]][args[1]][args[2]]:
            self.stat_pack[args[0]][args[1]][args[2]][args[3]] = {}

        if len(args) == 1:
            if 'count' not in self.stat_pack[args[0]]:
                self.stat_pack[args[0]]['count'] = 1
            else:
                self.stat_pack[args[0]]['count'] += 1
        elif len(args) == 2:
            if 'count' not in self.stat_pack[args[0]][args[1]]:
                self.stat_pack[args[0]][args[1]]['count'] = 1
            else:
                self.stat_pack[args[0]][args[1]]['count'] += 1
        elif len(args) == 3:
            if 'count' not in self.stat_pack[args[0]][args[1]][args[2]]:
                self.stat_pack[args[0]][args[1]][args[2]]['count'] = 1
            else:
                self.stat_pack[args[0]][args[1]][args[2]]['count'] += 1
        elif len(args) == 4:
            if 'count' not in self.stat_pack[args[0]][args[1]][args[2]][args[3]]:
                self.stat_pack[args[0]][args[1]][args[2]][args[3]]['count'] = 1
            else:
                self.stat_pack[args[0]][args[1]][args[2]][args[3]]['count'] += 1

        if 'value' in kwargs:
            value = kwargs['value']
            if len(args) == 1:
                if 'value' not in self.stat_pack[args[0]]:
                    self.stat_pack[args[0]]['value'] = [value]
                elif len(self.stat_pack[args[0]]['value']) < 10 and value not in self.stat_pack[args[0]]['value']:
                    self.stat_pack[args[0]]['value'].append(value)
            elif len(args) == 2:
                if 'value' not in self.stat_pack[args[0]][args[1]]:
                    self.stat_pack[args[0]][args[1]]['value'] = [value]
                elif len(self.stat_pack[args[0]][args[1]]['value']) < 10 and value not in self.stat_pack[args[0]][args[1]]['value']:
                    self.stat_pack[args[0]][args[1]]['value'].append(value)
            elif len(args) == 3:
                if 'value' not in self.stat_pack[args[0]][args[1]][args[2]]:
                    self.stat_pack[args[0]][args[1]][args[2]]['value'] = [value]
                elif len(self.stat_pack[args[0]][args[1]][args[2]]['value']) < 10 and value not in self.stat_pack[args[0]][args[1]][args[2]]['value']:
                    self.stat_pack[args[0]][args[1]][args[2]]['value'].append(value)
            elif len(args) == 4:
                if 'value' not in self.stat_pack[args[0]][args[1]][args[2]][args[3]]:
                    self.stat_pack[args[0]][args[1]][args[2]][args[3]]['value'] = [value]
                elif len(self.stat_pack[args[0]][args[1]][args[2]][args[3]]['value']) < 10 and value not in self.stat_pack[args[0]][args[1]][args[2]][args[3]]['value']:
                    self.stat_pack[args[0]][args[1]][args[2]][args[3]]['value'].append(value)

    def capture_mapped_stats(self, json_data):

        data_source = json_data.get('DATA_SOURCE', 'UNKNOWN')
        for key1 in json_data.keys():
            if isinstance(json_data[key1], list):
                self.update_stat(data_source, key1, value=json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, value=subrecord[key2])

def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True

if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False
    signal.signal(signal.SIGINT, signal_handler)

    input_file = '<input_file_name>'
    csv_dialect = '<dialect>'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n')
        sys.exit(1)

    base_file_name, file_extension = os.path.splitext(args.input_file)
    compressed_file = file_extension.upper() == '.GZ'
    if compressed_file:
        base_file_name, file_extension = os.path.splitext(base_file_name)
        input_file_handle = gzip.open(args.input_file, 'r')
        file_reader = io.TextIOWrapper(io.BufferedReader(input_file_handle), encoding='utf-8', errors='ignore')
    else:
        input_file_handle = open(file_name, 'r')
        file_reader = input_file_handle

    mapper = mapper()

    output_cache = {}

    input_row_count = 0
    for line in file_reader:
        input_row_count += 1
        input_row = json.loads(line)
        json_data = mapper.map(input_row, input_row_count)

        if json_data:
            if json_data['RECORD_ID'] not in output_cache:
                output_cache[json_data['RECORD_ID']] = json_data
            else:
                for attr in json_data.keys():
                    if attr == 'RELATIONSHIP_LIST':
                        output_cache[json_data['RECORD_ID']]['RELATIONSHIP_LIST'].extend(json_data['RELATIONSHIP_LIST'])
                    elif attr not in output_cache[json_data['RECORD_ID']]:
                        output_cache[json_data['RECORD_ID']][attr] = json_data[attr]

        if input_row_count % 10000 == 0:
            print(f'{input_row_count:,} rows processed')
        if shut_down:
            break

    if output_cache:
        output_file_name = args.output_file
        if output_file_name.endswith('.gz'):
            output_file_handle = gzip.open(output_file_name, 'wb')
        else:
            output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        output_row_count = 0
        for record_id in output_cache.keys():
            mapper.capture_mapped_stats(output_cache[record_id])
            if output_file_name.endswith('.gz'):
                output_file_handle.write((json.dumps(output_cache[record_id])+'\n').encode('utf-8'))
            else:
                output_file_handle.write((json.dumps(output_cache[record_id])+'\n'))
            output_row_count += 1
            if output_row_count % 10000 == 0:
                print(f'{output_row_count:,} rows written')
        print(f'{output_row_count:,} rows written. complete')

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file:
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)

    sys.exit(0)
