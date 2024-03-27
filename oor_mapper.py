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
        self.conversions['ADDR_TYPE'] = {'personStatement': {}, 'entityStatement': {}}
        self.conversions['ADDR_TYPE']['personStatement']['REGISTERED'] = 'PRIMARY'
        self.conversions['ADDR_TYPE']['entityStatement']['REGISTERED'] = 'BUSINESS'
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

        json_data['DATA_SOURCE'] = 'OPEN-OWNERSHIP'
        if statement_type == 'entityStatement':
            json_data['RECORD_ID'] = raw_data['statementID']
            json_data['RECORD_TYPE'] = 'ORGANIZATION'
            json_data = self.map_entity(raw_data, json_data)

        elif statement_type == 'personStatement':
            json_data['RECORD_ID'] = raw_data['statementID']
            json_data['RECORD_TYPE'] = 'PERSON'
            json_data = self.map_person(raw_data, json_data)

        elif statement_type == 'ownershipOrControlStatement':
            entity_type = list(raw_data['interestedParty'].keys())[0]
            json_data['RECORD_ID'] = raw_data['subject']['describedByEntityStatement']
            json_data = self.map_relationship(raw_data, json_data)

        for attr in raw_data.keys():
            mapper.update_stat('!raw', 'statement_attrs', statement_type, attr)

        #json_data = self.remove_empty_tags(json_data)

        return json_data

    def map_entity(self, raw_data, json_data):
        json_data['NAMES'] = [{'PRIMARY_NAME_ORG': raw_data.get('name')}]
        if raw_data.get('alternateNames'):
            for name_value in raw_data.get('alternateNames'):
                json_data['NAMES'].append({'ALTERNATE_NAME_FULL': name_value})

        if raw_data.get('foundingDate'):
            json_data['REGISTRATION_DATE'] = raw_data.get('foundingDate')

        if raw_data.get('dissolutionDate'):
            json_data['DISSOLVED'] = raw_data.get('dissolutionDate')

        if raw_data.get('incorporatedInJurisdiction'):
            json_data['REGISTRATION_COUNTRY'] = raw_data.get('incorporatedInJurisdiction').get('code')

        if raw_data.get('addresses'):
            json_data['ADDRESSES'] = self.map_addresses(raw_data)

        if raw_data.get('identifiers'):
            identifiers, links = self.map_identifiers(raw_data) 
            if identifiers:
                json_data['IDENTIFIERS'] = identifiers
            if links:
                json_data['LINKS'] = links

        json_data['RELATIONSHIPS'] = [{'REL_ANCHOR_DOMAIN': 'OOR', 'REL_ANCHOR_KEY': raw_data['statementID']}]

        return json_data

    def map_person(self, raw_data, json_data):

        json_data['NAMES'] = []
        for name_data in raw_data.get('names',[]):
            name_value = name_data.get('fullName')
            if name_value:
                raw_name_type = name_data.get('type', 'ALTERNATE').replace('_', '-')
                mapper.update_stat('!raw', 'name_type', 'PERSON', raw_name_type)
                if 'PRIMARY_NAME_FULL' not in json_data:
                    json_data['PRIMARY_NAME_FULL'] = name_value
                else:
                    json_data['NAMES'].append({f'{raw_name_type}_NAME_FULL': name_value})

        if raw_data.get('personType'):
            json_data['PERSON_TYPE'] = raw_data.get('personType')

        if raw_data.get('birthDate') or raw_data.get('nationalities'):
            json_data['ATTRIBUTES'] = []
            for nationality_data in raw_data.get('nationalities', []):
                json_data['ATTRIBUTES'].append({"NATIONALITY": nationality_data.get('code')})

            if raw_data.get('birthDate'):
                json_data['DATE_OF_BIRTH'] = raw_data.get('birthDate')

        if raw_data.get('addresses'):
            json_data['ADDRESSES'] = self.map_addresses(raw_data)

        if raw_data.get('identifiers'):
            identifiers, links = self.map_identifiers(raw_data) 
            if identifiers:
                json_data['IDENTIFIERS'] = identifiers
            if links:
                json_data['LINKS'] = links

        json_data['RELATIONSHIPS'] = [{'REL_ANCHOR_DOMAIN': 'OOR', 'REL_ANCHOR_KEY': raw_data['statementID']}]

        return json_data


    def map_addresses(self, raw_data):
        statement_type = raw_data.get('statementType')
        address_list = []
        for addr_data in raw_data.get('addresses',[]):
            addr_full = addr_data.get('address')
            if addr_full:
                addr_country = addr_data.get('country','')
                raw_addr_type = addr_data.get('type','unknown').upper()
                mapper.update_stat('!raw', 'address_type', raw_data.get('statementType', 'none'), raw_addr_type)
                addr_type = self.conversions['ADDR_TYPE'][statement_type].get(raw_addr_type, raw_addr_type  )
                address_list.append({'ADDR_TYPE': addr_type, 'ADDR_FULL': addr_full, 'ADDR_COUNTRY': addr_country})
                unspecified_addr_type = 'OTHER'
        return address_list


    def map_identifiers(self, raw_data):
        identifiers = []
        links = []
        for id_data in raw_data.get('identifiers',[]):
            id_value = id_data.get('id')
            id_uri = id_data.get('uri')
            scheme = id_data.get('scheme','')
            schemeName = id_data.get('schemeName','')
            if id_uri:
                mapper.update_stat('!raw', 'link', raw_data.get('statementType', 'none'), f'{schemeName}|{scheme}', value=id_value)
                if schemeName == 'OpenOwnership Register' and id_uri.startswith('/entities'):
                    id_uri = 'https://register.openownership.org' + id_uri
                links.append({schemeName: id_uri})
            else:
                senzing_attr, country = self.conversions['ID_TYPE'].get(scheme, ('NATIONAL_ID', ''))
                mapper.update_stat('!raw', 'identifier', raw_data.get('statementType', 'none'), f'{schemeName}|{scheme}|{senzing_attr}', value=id_value)
                if senzing_attr == 'NATIONAL_ID':
                    mapped_data = {'NATIONAL_ID_NUMBER': id_value, 'NATIONAL_ID_TYPE': scheme, 'NATIONAL_ID_COUNTRY': country}
                elif senzing_attr == 'OTHER_ID':
                    mapped_data = {'OTHER_ID_NUMBER': id_value, 'OTHER_ID_TYPE': scheme if scheme else schemeName, 'OTHER_ID_COUNTRY': country}
                else:
                    mapped_data = {senzing_attr: id_value}
                identifiers.append(mapped_data)
        return identifiers, links


    def map_relationship(self, raw_data, json_data):

        if raw_data['interestedParty'].get('describedByPersonStatement'):
            rel_pointer_key = raw_data['interestedParty'].get('describedByPersonStatement')
        elif raw_data['interestedParty'].get('describedByEntityStatement'):
            rel_pointer_key = raw_data['interestedParty'].get('describedByEntityStatement')
        else:
            rel_pointer_key = 'unknown'

        relationship_list = []
        for interest_data in raw_data.get('interests'):
            rel_pointer_role = interest_data.get('type', 'unknown').replace('-', '_')
            if interest_data.get('share') and interest_data.get('share').get('exact'):
                rel_pointer_role += f"-{round(interest_data.get('share').get('exact'),2)}%"
            relationship = {'REL_POINTER_DOMAIN': 'OOR', 'REL_POINTER_KEY': rel_pointer_key, 'REL_POINTER_ROLE': rel_pointer_role}
            if interest_data.get('startDate'):
                relationship['REL_POINTER_FROM_DATE'] = interest_data.get('startDate')
            if interest_data.get('endDate'):
                relationship['REL_POINTER_THRU_DATE'] = interest_data.get('endDate')
            relationship_list.append(relationship)

        json_data['RELATIONSHIPS'] = relationship_list
        if not relationship_list:
            mapper.update_stat('!alert', 'no-relationship-interests!', value=rel_pointer_key)
        return json_data


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

        record_type = json_data.get('RECORD_TYPE', 'UNKNOWN_TYPE')

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(record_type, key1, value=json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(record_type, key2, value=subrecord[key2])


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
        # fixed unknown variable input_file_name
        input_file_handle = open(args.input_file, 'r')
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
                    if attr == 'RELATIONSHIPS':
                        output_cache[json_data['RECORD_ID']]['RELATIONSHIPS'].extend(json_data['RELATIONSHIPS'])
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
            if output_cache[record_id].get('RECORD_TYPE'):
                mapper.capture_mapped_stats(output_cache[record_id])
                if output_file_name.endswith('.gz'):
                    output_file_handle.write((json.dumps(output_cache[record_id])+'\n').encode('utf-8'))
                else:
                    output_file_handle.write((json.dumps(output_cache[record_id])+'\n'))
                output_row_count += 1
                if output_row_count % 10000 == 0:
                    print(f'{output_row_count:,} rows written')
            else:
                mapper.update_stat('!alert', 'relationship-without-entity!', value=output_cache[record_id].get('RECORD_ID'))

        print(f'{output_row_count:,} rows written. complete')

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print(f'{input_row_count:,} rows processed, {output_row_count:,} rows written, {run_status}\n')

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file:
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)

    sys.exit(0)
