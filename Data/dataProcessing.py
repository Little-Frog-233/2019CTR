#coding:utf-8
import os
import time
import pandas as pd
import numpy as np
import calendar
from Data.dataConfig import dataConfig

config = dataConfig()
file_path = config.file_path

def totalExposureLog_to_csv_demo():
	totalExposureLog_path = os.path.join(file_path, 'totalExposureLog.out')
	count = 0
	all = 0
	weeks = {}
	hours = {}
	with open(totalExposureLog_path) as f:
		for i, line in enumerate(f):
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			ctr = float(lines[7])
			if ctr > 0.5:
				count += 1
			timestrip = int(lines[1])
			timeArray = time.localtime(timestrip)
			real_time = time.strftime("%Y %m %d %H %M %S", timeArray)
			real_time = real_time.split(' ')
			year = int(real_time[0])
			month = int(real_time[1])
			day = int(real_time[2])
			hour = int(real_time[3])
			if hour in hours.keys():
				hours[hour] += 1
			else:
				hours[hour] = 1
			week = calendar.weekday(year, month=month, day=day)
			if week in weeks.keys():
				weeks[week] += 1
			else:
				weeks[week] = 1
			if i % 1000000 == 0:
				print(real_time)
				real_time = ':'.join(real_time)
				print(lines)
				print(real_time)
		print(count,all,weeks,hours)

def read_ad_operation():
	'''
	读取ad_operation数据并转化为dataframe
	:return:
	'''
	ad_operation_path = os.path.join(file_path, 'ad_operation.dat')
	ad_operation_data = []
	with open(ad_operation_path) as f:
		for cnt, line in enumerate(f):
			data_error = '0'
			ad_op = {}
			line = line.strip('\n')
			datas = line.split('\t')
			for i,data in enumerate(datas):
				if config.ad_operation_columns[i] == 'create/change_time':
					if data != '0':
						year = int(data[0:4])
						mouth = int(data[4:6])
						day = int(data[6:8])
						hour = int(data[8:10])
						minute = int(data[10:12])
						second = int(data[12:])
						data = '%s-%s-%s %s:%s:%s' % (year, mouth, day, hour, minute, second)
						if year % 4 != 0 and mouth == 2 and day > 28:
							data = ''
							data_error = '1'
					elif data == '0':
						data = ''
					# elif data == '0':
					# 	data = None
				ad_op[config.ad_operation_columns[i]] = str(data)
			ad_op['data_error'] = data_error
			ad_operation_data.append(ad_op)
	ad_operation_df = pd.DataFrame(ad_operation_data)
	# ad_operation_df.sort_values(by=['ad_id','create/change_time'])
	ad_operation_df.to_csv(os.path.join(config.file_path,'ad_operation.csv'),index=None)
	return ad_operation_df

def read_ad_static():
	'''
	读取ad_static数据并转化为dataframe
	:return:
	'''
	ad_static_path = os.path.join(file_path, 'ad_static_feature.out')
	ad_static_data = []
	with open(ad_static_path) as f:
		for cnt, line in enumerate(f):
			ad_op = {}
			line = line.strip('\n')
			datas = line.split('\t')
			for i,data in enumerate(datas):
				if config.ad_static_feature_columns[i] == 'create_time':
					timeArray = time.localtime(int(data))
					data = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
				ad_op[config.ad_static_feature_columns[i]] = str(data)
			ad_static_data.append(ad_op)
	ad_static_df = pd.DataFrame(ad_static_data)
	ad_static_df.to_csv(os.path.join(config.file_path,'ad_static_feature.csv'),index=None)
	return ad_static_df

def user_data_to_csv():
	totalExposureLog_path = os.path.join(file_path, 'user_data')
	all = 0
	field = 0
	user_data = []
	with open(totalExposureLog_path) as f:
		for cnt, line in enumerate(f):
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			datas = {}
			for i,data in enumerate(lines):
				if data == '0':
					data = ''
				datas[config.user_feature_columns[i]] = data
			user_data.append(datas)
			if cnt % 200000 == 0 and cnt != 0:
				user_feature_df = pd.DataFrame(user_data)
				user_feature_df.to_csv(os.path.join(config.file_path, 'user_feature_%s.csv'%field), index=None)
				field += 1
				user_data = []
				# print(real_time)
		print(all)

def totalExposureLog_to_csv():
	totalExposureLog_path = os.path.join(file_path, 'totalExposureLog.out')
	all = 0
	hours_data = {}
	hour_list = ['6','6_5','7','7_5','8','8_5','9','9_5','10','10_5','11','11_5','12','12_5','13','13_5','14','14_5','15','15_5','16','16_5','17','17_5','18','18_5','19','19_5','20','20_5','21','21_5','22','22_5','23','23_5','0','0_5','1','1_5','2','2_5','3','3_5','4','4_5','5','5_5']
	for h_l in hour_list:
		hours_data[h_l] = []
	field = 0
	with open(totalExposureLog_path) as f:
		for cnt, line in enumerate(f):
			datas = {}
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			for i,data in enumerate(lines):
				datas[config.totalExposureLog_columns[i]] = data
			timestrip = int(lines[1])
			timeArray = time.localtime(timestrip)
			real_time = time.strftime("%Y %m %d %H %M %S", timeArray)
			real_times = real_time.split(' ')
			year = int(real_times[0])
			month = int(real_times[1])
			day = int(real_times[2])
			hour = int(real_times[3])
			minute = int(real_times[4])
			week = calendar.weekday(year, month=month, day=day)
			datas['week'] = week
			datas['day'] = day
			datas['month'] = month
			datas['hour'] = hour
			datas['minute'] = minute
			if minute >= 30:
				hours_data['%s_5'%hour].append(datas)
			else:
				hours_data['%s'%hour].append(datas)
			if cnt % 5000000 == 0 and cnt != 0:
				print(cnt)
				for item in hours_data.keys():
					print(item)
					df = pd.DataFrame(hours_data[item])
					df.to_csv(os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv' % (item,field)), index=None)
				field += 1
				for h_l in hour_list:
					hours_data[h_l] = []
	# for item in hours_data.keys():
	# 	print(item)
	# 	df = pd.DataFrame(hours_data[item])
	# 	df.to_csv(os.path.join(config.file_path, 'totalExposureLog_%s.csv'%item), index=None)

def totalExposureLog_read_df(hour=None):
	'''

	:param hour:
	:return:
	'''
	df = pd.concat(
            [pd.read_csv(os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv'%(hour,i))) for i in range(20)]).reset_index(drop=True)
	return df

def read_test_sample():
	test_sample_path = os.path.join(file_path, 'test_sample.dat')
	test_sample_data = []
	with open(test_sample_path) as f:
		for cnt, line in enumerate(f):
			datas = {}
			line = line.strip('\n')
			lines = line.split('\t')
			for i, data in enumerate(lines):
				datas[config.test_columns[i]] = data
			test_sample_data.append(datas)
	test_sample_df = pd.DataFrame(test_sample_data)
	return test_sample_df

def read_week_time_exposure(hour=None):
	df = pd.read_csv(os.path.join(config.file_path, 'weekTimeResult/result_%s.csv' % hour))
	for l in df['week','ad_bid']:
		print(l)




if __name__ == '__main__':
	# totalExposureLog_to_csv()

	# hour_list = ['6','6_5','7','7_5','8','8_5','9','9_5','10','10_5','11','11_5','12','12_5','13','13_5','14','14_5','15','15_5','16','16_5','17','17_5','18','18_5','19','19_5','20','20_5','21','21_5','22','22_5','23','23_5','0','0_5','1','1_5','2','2_5','3','3_5','4','4_5','5','5_5']
	# for i in hour_list:
	# 	df = totalExposureLog_read_df(i)
	# 	# df = df.groupby(['month','day','week','ad_id']).count()
	# 	# df.to_csv(os.path.join(config.file_path, 'totalResult/result_%s.csv'%i))
	# 	df = df.groupby(['month', 'day', 'week', 'ad_id']).count().groupby(['month', 'day', 'week']).mean().groupby(
	# 		'week').mean()
	# 	df.to_csv(os.path.join(config.file_path, 'weekTimeResult/result_%s.csv' % i))

	# df = read_test_sample()
	# print(df['feed_time'][0])

	read_week_time_exposure()
