############################################################################################
######this script allow you to automatically create status and user table for weibo data####
######and dump raw weibo data into postgresql database#######################################
#######author: Guixing Wei Email:g_w38@txstate.edu###########################################
########before you use this sctipt, pls create a database called weibo#######################
__author__ = 'wgx'
import json
import re
import glob
import psycopg2
import copy


class weibo_parse():
    def __init__(self):
        self.working_dir = r"your data directory/*.json"#change to your own data directory
        self.host = 'localhost'
        self.database = 'weibo'
        self.user = 'postgres'
        self.password = 'pgsql2015'

    def create_table_statuses(self):
        try:
            conn = psycopg2.connect(host=self.host,database=self.database,user=self.user,password=self.password)
            cur = conn.cursor()
            cur.execute('CREATE SCHEMA IF NOT EXISTS test;')
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public version '2.1.8';")
            conn.commit()
            cur.execute(
                'CREATE TABLE IF NOT EXISTS test.statuses('
                'id CHARACTER VARYING(30) NOT NULL,'
                'uid CHARACTER VARYING(30) NOT NULL,'
                'reposts_count INTEGER, '
                'in_reply_to_status_id CHARACTER VARYING(30),'
                'in_reply_to_user_id CHARACTER VARYING(30),'
                'source_type INTEGER,'
                'statuses_count INTEGER,'
                'verified BOOLEAN,'
                'followers_count INTEGER,'
                'created_at TIMESTAMP WITH TIME ZONE,'
                'geo_type CHARACTER VARYING(10),'
                'geo_lon DOUBLE PRECISION,'
                'geo_lat DOUBLE PRECISION,'
                'geo_coordinates geography(Point),'
                'comments_count INTEGER,'
                'raw json,'
                #'like_count INTEGER,'
                'CONSTRAINT id_key PRIMARY KEY (id),'
                'CONSTRAINT foreign_uid FOREIGN KEY (uid)'
                'REFERENCES test.users (uid) MATCH SIMPLE '
                'ON UPDATE NO ACTION ON DELETE RESTRICT)'
                'WITH (  OIDS=TRUE);'
            )
            conn.commit()
        except Exception as err:
             print "---> error happened during table statuses creation"
             print err.message
        finally:
            if conn:
                conn.close()

    def create_table_users(self):
        try:
            conn = psycopg2.connect(host=self.host,database=self.database,user=self.user,password=self.password)
            cur = conn.cursor()
            cur.execute('CREATE SCHEMA IF NOT EXISTS test;')
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public version '2.1.8';")
            conn.commit()
            cur.execute('CREATE TABLE IF NOT EXISTS test.users('
                        'uid character varying(30) NOT NULL,'
                        'gender character(1),'
                        'city integer,'
                        'province integer,'
                        'dob timestamp with time zone,'
                        'edu_prim character varying(20),'
                        'edu_second character varying(20),'
                        'edu_high character varying(20),'
                        'work_1 character varying(20),'
                        'work_1_start timestamp with time zone,'
                        'work_1_end timestamp with time zone,'
                        'work_2 character varying(20),'
                        'work_2_start timestamp without time zone,'
                        'work_2_end character varying(20),'
                        'CONSTRAINT uni_uid UNIQUE (uid))'
                        'WITH (  OIDS=TRUE);')
            conn.commit()
        except Exception as err:
            print "-------> error happened during table users creation"
            print err.message
        finally:
            if conn:
                conn.close()


    def statuses_insert(self, data_list_dict):
        conn = None
        try:
            conn = psycopg2.connect(host=self.host,database=self.database,user=self.user,password=self.password)
            cur = conn.cursor()
            try:
                cur.executemany('INSERT INTO test.users (uid,gender,city, province) SELECT %(uid)s, %(gender)s, %(city)s, %(province)s WHERE NOT EXISTS (SELECT * FROM test.users WHERE test.users.uid =%(uid)s)', data_list_dict)
                conn.commit()
                print "insert users_table successfully"
            except Exception as err_user:
                print "----> error happened during users data insertation"
                print err_user.message
            cur.executemany('INSERT INTO test.statuses (id, uid, reposts_count,in_reply_to_status_id, in_reply_to_user_id, '
                        'source_type, statuses_count, verified, followers_count, created_at, geo_type,'
                        'geo_lon, geo_lat, comments_count) SELECT %(id)s,%(uid)s, %(reposts_count)s, %(in_reply_to_status_id)s, '
                        '%(in_reply_to_user_id)s, %(source_type)s, %(statuses_count)s, %(verified)s, %(followers_count)s,'
                        '%(created_at)s, %(geo_type)s, %(geo_lon)s, %(geo_lat)s, %(comments_count)s WHERE NOT EXISTS (SELECT * FROM test.statuses as sta WHERE sta.id = %(id)s)', data_list_dict)

            cur.execute('UPDATE test.statuses SET '
                        'geo_coordinates=ST_SETSRID(ST_MAKEPOINT(geo_lon,geo_lat),4326) where geo_coordinates is Null')
            print "insert statuses_table successfully"
            conn.commit()

        except Exception as err:
            print "---> error happened during statuses data insertation"
            print err.message
        finally:
            if conn:
                conn.close()

    def time_format(self, date_time):
        words = date_time.split()
        self.time_input = words[1]+'-'+ words[2]+'-'+ words[5] + ' '+ words[3]+words[4][:3]

    def add_endBracket(self):
        """
        this function is designed to add a "{" at the end of weibo files
        """
        for filename in glob.glob(self.working_dir):
             with open(filename,'a+') as write_file:
                 write_file.seek(-1,2)
                 end_char = write_file.read(1)
                 if end_char =='}':
                     write_file.seek(0,2)
                     write_file.write('{')


    def pattern_parse(self):
    #data_pattern = re.compile(r'(\{\n.*?\})(?=\{)')

        data_pattern = re.compile(r'({\n[ ]{4}.+?^})(?={\n?)', re.DOTALL | re.MULTILINE)
        data_list_dict = []
        self.data_dict = {}
        for filename in glob.glob(self.working_dir):
            with open(filename) as data_file:
                print ("--->the current processed file is"+filename)
                str_json = data_file.read()
                matches = re.findall(data_pattern,str_json)
                for match in matches:
                    data = json.loads(match)
                    counts = list(data['states']).__len__()
                    for count in range(0,counts,1):
                        if (data['statuses'][count]).has_key('deleted'):
                            continue
                        if not (data['statuses'][count]).has_key('geo'):
                            continue
                        if not data['statuses'][count]['geo']:
                            continue

                        self.data_dict['id'] = str(data['statuses'][count]['id'])
                        self.data_dict['uid'] = str(data['statuses'][count]['user']['id'])
                        self.data_dict['reposts_count'] = data['statuses'][count]['reposts_count']
                        self.data_dict['in_reply_to_status_id'] = data['statuses'][count]['in_reply_to_status_id']
                        self.data_dict['in_reply_to_user_id'] = data['statuses'][count]['in_reply_to_user_id']
                        self.data_dict['source_type'] = data['statuses'][count]['source_type']
                        self.data_dict['statuses_count'] = data['statuses'][count]['user']['statuses_count']
                        self.data_dict['verified'] = data['statuses'][count]['user']['verified']
                        self.data_dict['followers_count'] = data['statuses'][count]['user']['followers_count']
                        self.data_dict['created_at'] = data['statuses'][count]['created_at']
                        self.data_dict['geo_type'] = data['statuses'][count]['geo']['type']
                        self.data_dict['geo_lon'] = data['statuses'][count]['geo']['coordinates'][1]
                        self.data_dict['geo_lat'] = data['statuses'][count]['geo']['coordinates'][0]
                        self.data_dict['city'] = int(data['statuses'][count]['user']['city'])
                        self.data_dict['province'] = int(data['statuses'][count]['user']['province'])

                        self.data_dict['comments_count'] = data['statuses'][count]['comments_count']
                        self.data_dict['gender'] = data['statuses'][count]['user']['gender']
                        #self.data_dict['raw'] = json.dumps(data['statuses'][count])
                        #data_dict['like_count'] = data['statuses'][count]['url_objects']['like_count']
                        self.time_format(self.data_dict['created_at'])
                        self.data_dict['created_at']  = self.time_input
                        data_list_dict.append(copy.deepcopy(self.data_dict))
                self.statuses_insert(data_list_dict)
                del data_list_dict[:]


if __name__ == "__main__":
    parse_obj = weibo_parse()
    #parse_obj.create_table_users()
    #parse_obj.create_table_statuses()
    parse_obj.add_endBracket()
    parse_obj.pattern_parse()




