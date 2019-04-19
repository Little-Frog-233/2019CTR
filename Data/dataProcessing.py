#coding:utf-8
import os
import time
import pandas as pd
import numpy as np
import calendar
from Data.dataConfig import dataConfig

config = dataConfig()
file_path = config.file_path

def totalExposureLog_to_csv():
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
	nuLL = {}
	with open(totalExposureLog_path) as f:
		for cnt, line in enumerate(f):
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			datas = {}
			for i,data in enumerate(lines):
				if data == '0':
					if config.user_feature_columns[i] in nuLL.keys():
						nuLL[config.user_feature_columns[i]] += 1
					else:
						nuLL[config.user_feature_columns[i]] = 1
				datas[config.user_feature_columns[i]] = data
			if cnt % 200000 == 0 and cnt != 0:
				print(lines)
				field += 1
				# print(real_time)
		print(all)
		print('-'*40)
		print(nuLL)


if __name__ == '__main__':
	user_data_to_csv()
