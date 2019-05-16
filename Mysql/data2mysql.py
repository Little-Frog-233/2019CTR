#coding:utf-8
import os
import configparser
import pymysql

class data2mysql:
	def __init__(self):
		cf = configparser.ConfigParser()
		base_dir = os.path.dirname(__file__)
		cf.read(os.path.join(base_dir, "mysql.config"))
		try:
			mysql_config = dict(cf.items("mysql"))
		except Exception as e:
			mysql_config = None
		# sqlalchemy 基本变量
		if mysql_config:
			self.db = pymysql.connect(host=mysql_config.get('mysql_host'), user=mysql_config.get('mysql_user'),
			                          password=mysql_config.get('mysql_pass'), port=3306,
			                          db=mysql_config.get('mysql_db'), charset='utf8')
		else:
			self.db = pymysql.connect(host='127.0.0.1', user='root', password='***', port=3306, db='2019tencentCTR',
			                          charset='utf8')
		self.cursor = self.db.cursor()


	def find_ad_static_by_id(self,ad_id):
		'''
		根据广告id查找广告静态特征
		:return:
		'''
		sql = 'SELECT * from ad_static WHERE ad_id=%s' % ad_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return one
		else:
			print('didn\'t find id: ',ad_id)
			return None

	def find_user_by_id(self,user_id):
		'''
		根据用户id查找用户特征
		:return:
		'''
		pass

	def find_ad_operation_by_id_data(self,ad_id):
		'''
		根据广告id查找修改数据
		:param ad_id:
		:param data:
		:return:
		'''
		sql = 'SELECT * from ad_operation WHERE ad_id=%s' % ad_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return one
		else:
			print('didn\'t find id: ', ad_id)
			return None
		pass

	def find_ad_operation_by_id_data_user(self,ad_id):
		'''
		根据广告id查找修改数据
		:param ad_id:
		:param data:
		:return:
		'''
		sql = 'SELECT field_after_change from ad_operation WHERE ad_id=%s and change_field=3' % ad_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return one[0]
		else:
			# print('didn\'t find id: ', ad_id)
			return None

	def get_all_user_data_age(self):
		'''
		获取全部用户信息
		:return:
		'''
		sql = 'SELECT user_id, age from user_feature where age is not null'
		self.cursor.execute(sql)
		all_results = self.cursor.fetchall()
		if all_results:
			return [[i[0], set([str(i[1])])] for i in all_results]
		else:
			return None

	def get_all_user_data_gender(self):
		'''
		获取全部用户信息
		:return:
		'''
		sql = 'SELECT user_id, gender from user_feature'
		self.cursor.execute(sql)
		all_results = self.cursor.fetchall()
		if all_results:
			return [[i[0],i[1]] for i in all_results]
		else:
			return None

	def get_all_user_data_area(self):
		'''
		获取全部用户信息
		:return:
		'''
		sql = 'SELECT user_id, area from user_feature'
		self.cursor.execute(sql)
		all_results = self.cursor.fetchall()
		if all_results:
			return [(i[0], set(i[1].split(','))) for i in all_results]
		else:
			return None

if __name__ == '__main__':
	mysql = data2mysql()
	# print(mysql.find_ad_static_by_id(2)[1])
	# print(mysql.find_ad_operation_by_id_data_user(437633))
	all_user_data = mysql.get_all_user_data_area()
	print(all_user_data[:10])