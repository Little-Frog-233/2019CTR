# coding:utf-8
import sys
sys.path.append('/Users/ruicheng/Documents/上海师范研究生/腾讯2019广告算法大赛/TencentCTR')
import os
import time
import pandas as pd
import numpy as np
import calendar
import math
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from scipy import sparse
from Data.dataConfig import dataConfig
from Mysql.data2mysql import data2mysql

config = dataConfig()
file_path = config.file_path
mysql = data2mysql()
pd.set_option('display.width', None)


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
		print(count, all, weeks, hours)


def read_ad_operation(save=False):
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
			for i, data in enumerate(datas):
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
	if save:
		# ad_operation_df.sort_values(by=['ad_id','create/change_time'])
		ad_operation_df.to_csv(os.path.join(config.file_path, 'ad_operation.csv'), index=None)
	return ad_operation_df


def read_ad_static(save=False):
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
			for i, data in enumerate(datas):
				if config.ad_static_feature_columns[i] == 'industry_id':
					data = data.split(',')[0]
				if ',' in data and config.ad_static_feature_columns[i] != 'industry_id':
					data = None
				if data == '':
					data = None
				if config.ad_static_feature_columns[i] == 'create_time':
					timeArray = time.localtime(int(data))
					data = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
				ad_op[config.ad_static_feature_columns[i]] = data
			ad_static_data.append(ad_op)
	ad_static_df = pd.DataFrame(ad_static_data)
	if save:
		ad_static_df.to_csv(os.path.join(config.file_path, 'ad_static_feature.csv'), index=None)
	return ad_static_df


def user_data_to_csv():
	'''
	读取user_feature并转化成csv，每隔200000个数据进行存储
	:return:
	'''
	totalExposureLog_path = os.path.join(file_path, 'user_data')
	all = 0
	field = 0
	user_data = []
	empty = {}
	with open(totalExposureLog_path) as f:
		for cnt, line in enumerate(f):
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			datas = {}
			for i, data in enumerate(lines):
				if data == '0':
					data = ''
					if config.user_feature_columns[i] in empty.keys():
						empty[config.user_feature_columns[i]] += 1
					else:
						empty[config.user_feature_columns[i]] = 1
				datas[config.user_feature_columns[i]] = data
			user_data.append(datas)
			if cnt % 200000 == 0 and cnt != 0:
				user_feature_df = pd.DataFrame(user_data)
				user_feature_df.to_csv(os.path.join(config.file_path, 'userFeature/user_feature_%s.csv' % field),
				                       index=None)
				field += 1
				user_data = []
		if user_data != []:
			user_feature_df = pd.DataFrame(user_data)
			user_feature_df.to_csv(os.path.join(config.file_path, 'userFeature/user_feature_%s.csv' % field),
			                       index=None)
		# print(real_time)
		print(empty)


def read_user_feature():
	'''
	读取所有的用户csv并合并成dataframe
	:return:
	'''
	df = pd.concat(
		[pd.read_csv(os.path.join(config.file_path, 'userFeature/user_feature_%s.csv' % i)) for i
		 in range(10) if os.path.exists(
			os.path.join(config.file_path, 'userFeature/user_feature_%s.csv' % i))]).reset_index(
		drop=True)
	return df


def totalExposureLog_to_csv():
	'''
	读取totalExposureLog并存储成csv
	:return:
	'''
	totalExposureLog_path = os.path.join(file_path, 'totalExposureLog.out')
	all = 0
	hours_data = {}
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	for h_l in hour_list:
		hours_data[h_l] = []
	field = 0
	with open(totalExposureLog_path) as f:
		for cnt, line in enumerate(f):
			datas = {}
			all += 1
			line = line.strip('\n')
			lines = line.split('\t')
			for i, data in enumerate(lines):
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
				hours_data['%s_5' % hour].append(datas)
			else:
				hours_data['%s' % hour].append(datas)
			if cnt % 5000000 == 0 and cnt != 0:
				print(cnt)
				for item in hours_data.keys():
					print(item)
					df = pd.DataFrame(hours_data[item])
					df.to_csv(
						os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv' % (item, field)),
						index=None)
				field += 1
				for h_l in hour_list:
					hours_data[h_l] = []
		for item in hours_data.keys():
			print(item)
			if hours_data[item] != []:
				df = pd.DataFrame(hours_data[item])
				df.to_csv(
					os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv' % (item, field)),
					index=None)
			# for item in hours_data.keys():
			# 	print(item)
			# 	df = pd.DataFrame(hours_data[item])
			# 	df.to_csv(os.path.join(config.file_path, 'totalExposureLog_%s.csv'%item), index=None)


def totalExposureLog_read_df(hour=None):
	'''
	读取指定时间段的曝光数据并合并成dataframe
	:param hour:
	:return:
	'''
	df = pd.concat(
		[pd.read_csv(os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv' % (hour, i))) for i
		 in range(21) if os.path.exists(
			os.path.join(config.file_path, 'totalExposure/totalExposureLog_%s_field%s.csv' % (hour, i)))]).reset_index(
		drop=True)
	print('done')
	return df


def totalResult_to_csv():
	'''
	计算每个广告id在每天的固定时段的曝光量，并输出成csv
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	for i in hour_list:
		df = totalExposureLog_read_df(i)
		df = df.groupby(
			['month', 'day', 'week', 'ad_id', 'user_id', 'ad_requests_time', 'ad_location_id', 'ad_shape']).count()
		# df = df.groupby(['month', 'day', 'week', 'ad_id']).count()
		df.to_csv(os.path.join(config.file_path, 'totalResult/result_%s.csv' % i))
		print(i + ' done')


def totalExposure_day_to_csv():
	'''
	计算日曝光，根据month、day、ad_id、ad_shape进行groupby
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	df = pd.concat([totalExposureLog_read_df(hour=i) for i in hour_list]).reset_index(drop=True)
	df = df.groupby(['month', 'day', 'ad_id', 'ad_shape']).count()
	df.to_csv(os.path.join(config.file_path, 'totalExposure_day_result.csv'))


def totalExposure_day_read():
	'''
	读取日曝光文件
	:return:
	'''
	df = pd.read_csv(os.path.join(config.file_path, 'totalExposure_day_result.csv'))
	df['daily_exposure'] = df['ad_bid']
	df = df[['month', 'day', 'ad_id', 'ad_shape', 'daily_exposure']]
	return df


def totalExposure_day_ad_dict():
	'''
	读取每个id的历史均曝光，用于历史广告id的曝光填充
	:return:
	'''
	df = totalExposure_day_read()
	ad_exposure_dict = {}
	for item in df[['ad_id', 'daily_exposure']].values:
		ad_id = int(item[0])
		ad_exposure = int(item[1])
		if ad_id in ad_exposure_dict.keys():
			ad_exposure_dict[ad_id].append(ad_exposure)
		else:
			ad_exposure_dict[ad_id] = [ad_exposure]
	for ad_id in ad_exposure_dict.keys():
		ad_exposure_dict[ad_id] = np.mean(ad_exposure_dict[ad_id])
	return ad_exposure_dict


def totalResult_bid_to_csv():
	'''
	计算每个广告id，根据bid分组，在每天的固定时段的曝光量，并输出成csv
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	for i in hour_list:
		df = totalExposureLog_read_df(i)
		df = df.groupby(['month', 'day', 'week', 'ad_id', 'ad_bid']).count()
		df.to_csv(os.path.join(config.file_path, 'totalResult_bid/result_%s.csv' % i))


def totalResult_bid_week_to_csv():
	'''
	计算每天的曝光，根据bid分组，在每天的固定时段的曝光量，并输出成csv
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	for i in hour_list:
		df = totalExposureLog_read_df(i)
		df = df.groupby(['month', 'day', 'week', 'ad_id', 'ad_bid']).count().groupby(
			['month', 'day', 'week', 'ad_bid']).mean().groupby(
			['week', 'ad_bid']).mean()
		# df = df.groupby(['month', 'day', 'week', 'ad_bid']).count().groupby(
		# 	['month', 'day', 'week', 'ad_bid']).mean().groupby(
		# 	['week', 'ad_bid']).mean()
		df.to_csv(os.path.join(config.file_path, 'totalResult_bid/result_%s.csv' % i))


def weekTimeResult_to_csv():
	'''
	计算每天的固定时段的平均曝光量，并输出成csv
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	for i in hour_list:
		df = totalExposureLog_read_df(i)
		df = df.groupby(['month', 'day', 'week', 'ad_id']).count().groupby(['month', 'day', 'week']).mean().groupby(
			'week').mean()
		df.to_csv(os.path.join(config.file_path, 'weekTimeResult/result_%s.csv' % i))


def read_test_sample(rank_B=False):
	'''
	读取测试数据，转化为dataframe
	:return:
	'''
	if not rank_B:
		test_sample_path = os.path.join(file_path, 'test_sample.dat')
	else:
		test_sample_path = os.path.join(file_path, 'Btest_sample_new.dat')
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
	'''

	:param hour:
	:return:
	'''
	df = pd.read_csv(os.path.join(config.file_path, 'weekTimeResult/result_%s.csv' % hour))
	df_v = df.values
	week_exposure = {}
	for i in df_v:
		week_exposure[int(i[0])] = i[1]
	return week_exposure


def read_week_time_bid_exposure(hour=None):
	'''

	:param hour:
	:return:
	'''
	df = pd.read_csv(os.path.join(config.file_path, 'totalResult_bid/result_%s.csv' % hour))
	df_v = df.values
	week_bid_exposure = {}
	for i in df_v:
		if int(i[0]) in week_bid_exposure.keys():
			temp = week_bid_exposure[int(i[0])]
			temp[int(i[1])] = i[2]
			week_bid_exposure[int(i[0])] = temp
		else:
			temp = {}
			temp[int(i[1])] = i[2]
			week_bid_exposure[int(i[0])] = temp
	return week_bid_exposure


def read_all_week_time_exposure():
	'''

	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	time_exposure = {}
	for hour in hour_list:
		time_exposure[hour] = read_week_time_exposure(hour)
	return time_exposure


def read_all_week_time_bid_exposure():
	'''

	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	time_exposure = {}
	for hour in hour_list:
		time_exposure[hour] = read_week_time_bid_exposure(hour)
	return time_exposure


def test_feed_time_process_week(feed_time_str, week, time_exposure):
	'''

	:param feed_time_str:
	:param week:
	:param time_exposure:
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	feed_time = bin(int(feed_time_str)).strip('0b')
	result = 0
	for time_index, time_value in enumerate(feed_time):
		if time_value == '1':
			result += time_exposure[hour_list[time_index]][week]
	return result


def test_feed_time_process_week_bid(feed_time_str, week, time_bid_exposure, bid):
	'''

	:param feed_time_str:
	:param week:
	:param time_exposure:
	:return:
	'''
	hour_list = ['6', '6_5', '7', '7_5', '8', '8_5', '9', '9_5', '10', '10_5', '11', '11_5', '12', '12_5', '13', '13_5',
	             '14', '14_5', '15', '15_5', '16', '16_5', '17', '17_5', '18', '18_5', '19', '19_5', '20', '20_5', '21',
	             '21_5', '22', '22_5', '23', '23_5', '0', '0_5', '1', '1_5', '2', '2_5', '3', '3_5', '4', '4_5', '5',
	             '5_5']
	feed_time = bin(int(feed_time_str)).strip('0b')
	result = 0
	for time_index, time_value in enumerate(feed_time):
		if time_value == '1':
			if int(bid) in time_bid_exposure[hour_list[time_index]][week].keys():
				result += time_bid_exposure[hour_list[time_index]][week].get(int(bid))
			else:
				while int(bid) not in time_bid_exposure[hour_list[time_index]][week].keys():
					bid = int(bid) - 1
				result += time_bid_exposure[hour_list[time_index]][week].get(int(bid))
	return result


def test_feed_time_process(time_str):
	'''

	:param time_str:
	:return:
	'''
	global time_exposure
	result = 0
	# time_exposure = read_all_week_time_exposure()
	time_lists = time_str.split(',')
	for week, time_feed in enumerate(time_lists):
		result += test_feed_time_process_week(time_feed, week, time_exposure)
	return round(result / 7, 4)


def test_feed_time_process_bid(time_str, bid):
	'''

	:param time_str:
	:return:
	'''
	global time_exposure
	result = 0
	# time_exposure = read_all_week_time_exposure()
	time_lists = time_str.split(',')
	for week, time_feed in enumerate(time_lists):
		result += test_feed_time_process_week_bid(time_feed, week, time_exposure, bid)
	return round(result / 7, 4)


def test_baseline():
	'''
	网上的baseline
	:return:
	'''
	testA = read_test_sample()
	testA.set_index('sample_id')[['ad_id', 'ad_bid']].groupby('ad_id')['ad_bid'].apply(
		lambda row: pd.Series(dict(zip(row.index, row.rank() / 6)))).round(4).to_csv(
		os.path.join(config.file_path, 'submission.csv'), header=None)


def totalExposure_ad_user(hour=None, totalExposure_columns=None, adStatic_columns=None, userFeature_columns=None):
	'''

	:return:data(pd.DataFrame)
	'''
	ad_df = read_ad_static()
	user_df = read_user_feature()
	totalExposure_df = totalExposureLog_read_df(hour=hour)

	###处理曝光数据
	if totalExposure_columns:
		totalExposure_df = totalExposure_df[totalExposure_columns]
	else:
		totalExposure_df = totalExposure_df[['ad_id', 'user_id', 'ad_shape', 'week', 'ad_bid', 'ad_pctr',
		                                     'ad_quality_ecpm']]
	print(totalExposure_df.isnull().sum())

	###处理广告数据，commity_id空缺行补0，source_type补64
	print(ad_df.isnull().sum())
	print(ad_df.describe())
	ad_df['commodity_id'] = ad_df['commodity_id'].fillna(0)
	if adStatic_columns:
		ad_df = ad_df[adStatic_columns]
	else:
		ad_df = ad_df[['ad_id', 'industry_id', 'commodity_type', 'commodity_id', 'account_id']]
	# ad_df = ad_df[~ad_df['commodity_id'].isin(['null'])]
	# print(ad_df.groupby('source_type').count())
	# ad_df['source_type'] = ad_df['source_type'].fillna(64)
	print(ad_df.isnull().sum())

	###处理用户数据
	print(user_df.isnull().sum())
	user_df['device'] = user_df['device'].fillna(2)
	print(user_df.isnull().sum())
	if userFeature_columns:
		user_df = user_df[userFeature_columns]
	else:
		user_df = user_df[['user_id', 'gender', 'education', 'consuption_ability', 'work', 'connection_type']]

	###合并数据
	data = pd.merge(totalExposure_df, ad_df, on='ad_id', how='left')
	data = pd.merge(data, user_df, on='user_id', how='left')
	return data


def month_day_jugge(month, day):
	if int(month) == 3 and int(day) == 19:
		return 2.0
	else:
		return 1.0


def temp(x):
	'''
	临时函数，查找是否有改动的信息
	:param x:
	:return:
	'''
	result = mysql.find_ad_operation_by_id_data_user(x)
	if result:
		return 1
	else:
		return np.NaN


def totalExposureDay_ad_user():
	'''
	筛选有用户定向信息的广告数据
	:param df:
	:return:
	'''
	data_df = totalExposure_day_read()
	# data_df['user_field'] = data_df['ad_id'].apply(int).apply(temp)
	ad_id = data_df['ad_id'].to_list()
	ad_id = set(ad_id)
	print('strat to find')
	empty_ad = set()
	ad_user = {}
	for i in ad_id:
		user_field = mysql.find_ad_operation_by_id_data_user(int(i))
		if not user_field:
			empty_ad.add(int(i))
		else:
			ad_user[int(i)] = user_field
	data_df['feed_people'] = data_df['ad_id'].apply(int).apply(lambda x: ad_user.get(x, None))
	print(data_df.isnull().sum())
	data_df = data_df.dropna(axis=0).reset_index(drop=True)
	print(data_df.head())
	return data_df


def analys_user_field_gender(user_field, user_data):
	'''

	:param user_field:
	:param user_data:
	:return:
	'''
	user_field_list = user_field.split('|')
	# user_id = []
	all_user = len(user_data)
	count = 0
	for user_field in user_field_list:
		user_field = user_field.split(':')
		if user_field[0] == 'all':
			return all_user
		if len(user_field) > 1:
			field = user_field[0]
			item = user_field[1].split(',')
			if field == 'gender':
				for user in user_data:
					if str(user[1]) in item:
						# user_id.append(int(user[0]))
						count += 1
	if count == 0:
		return 1.0
	return count / all_user


def analys_user_field_area(user_field, user_data):
	'''

	:param user_field:
	:param user_data:
	:return:
	'''
	global all_count
	all_count += 1
	user_field_list = user_field.split('|')
	# user_id = []
	all_user = len(user_data)
	count = 0
	for user_field in user_field_list:
		user_field = user_field.split(':')
		if user_field[0] == 'all':
			return 1.0
		if len(user_field) > 1:
			field = user_field[0]
			item = set(user_field[1].split(','))
			if field == 'area':
				for user in user_data:
					if len(user[1] & item) != 0:
						# user_id.append(int(user[0]))
						count += 1
	if count == 0:
		return 1.0
	if all_count % 100 == 0:
		print(all_count)
	return count / all_user


def analys_user_field_area_age(user_field, user_data):
	'''

	:param user_field:
	:param user_data:
	:return:
	'''
	global all_count
	all_count += 1
	user_field_list = user_field.split('|')
	# user_id = []
	all_user = len(user_data['area'])
	user_index = {}
	for field_name in user_data.keys():
		user_index[field_name] = []
		for user_field in user_field_list:
			user_field = user_field.split(':')
			if user_field[0] == 'all':
				return 1.0
			if len(user_field) > 1:
				field = user_field[0]
				item = set(user_field[1].split(','))
				if field == field_name:
					for user in user_data[field_name]:
						if len(user[1] & item) != 0:
							# user_id.append(int(user[0]))
							user_index[field_name].append(int(user[0]))
	all_index = set([i for i in range(all_user)])
	for field_name in user_index.keys():
		if len(user_index[field_name]) == 0:
			continue
		all_index = all_index & set(user_index[field_name])
	if all_count % 100 == 0:
		print(all_count, len(all_index) / all_user)
	if len(all_index) == 0:
		return 1.0
	return len(all_index) / all_user

def analys_user_field_by_dict(user_fields, user_dict):
	'''

	:param user_field:
	:param user_dict:
	:return:
	'''
	global all_count
	all_count += 1
	all_user = 1396718
	user_field_list = user_fields.split('|')
	user_index = {}
	for field_name in user_dict.keys():
		for user_field in user_field_list:
			user_field = user_field.split(':')
			if user_field[0] == 'all':
				return 1.0
			if len(user_field) > 1:
				field = user_field[0]
				item = ','.join(sorted(user_field[1].split(',')))
				if field  == field_name:
					user_index[field_name] = user_dict[field_name].get(item, set())

	all_index = set([i for i in range(all_user)])
	for field_name in user_index.keys():
		if len(user_index[field_name]) == 0:
			continue
		all_index = all_index & set(user_index[field_name])
	if all_count % 100 == 0:
		print(all_count, len(all_index) / all_user)
	if len(all_index) == 0:
		return 1.0
	return len(all_index) / all_user


def totalExposureDay_ad_train_test(adStatic_columns=None, save=False, user=False, user_data=None):
	'''
	将训练数据和测试数据按列名进行上下合并
	训练数据和广告静态数据进行merge
	:param adStatic_columns:
	:return:
	'''
	if not user:
		totalExposure_df = totalExposure_day_read()
	else:
		totalExposure_df = totalExposureDay_ad_user()
	if user:
		totalExposure_df = totalExposure_df[['ad_id', 'month', 'day', 'ad_shape', 'daily_exposure', 'feed_people']]
	else:
		totalExposure_df = totalExposure_df[['ad_id', 'month', 'day', 'ad_shape', 'daily_exposure']]
	totalExposure_df['ad_id'] = totalExposure_df['ad_id'].apply(int)
	# print(totalExposure_df.isnull().sum())
	# print('-'*40)

	test_df = read_test_sample()
	if user:
		test_df = test_df[
			['ad_id', 'ad_shape', 'industry_id', 'commodity_type', 'commodity_id', 'account_id', 'feed_people']]
	else:
		test_df = test_df[['ad_id', 'ad_shape', 'industry_id', 'commodity_type', 'commodity_id', 'account_id']]
	test_df['ad_id'] = test_df['ad_id'].apply(int)
	# print(test_df.isnull().sum())
	# print('-' * 40)

	ad_df = read_ad_static()
	ad_df['commodity_id'] = ad_df['commodity_id'].fillna(0)
	if adStatic_columns:
		ad_df = ad_df[adStatic_columns]
	else:
		ad_df = ad_df[['ad_id', 'industry_id', 'commodity_type', 'commodity_id', 'account_id']]
	ad_df['ad_id'] = ad_df['ad_id'].apply(int)
	# print(ad_df.isnull().sum())
	# print('-' * 40)

	totalData_df = pd.merge(totalExposure_df, ad_df, on='ad_id', how='left')
	# print(totalData_df.isnull().sum())
	# print('-' * 40)

	# 删除行之后记得重置index
	totalData_df = totalData_df.dropna(axis=0).reset_index(drop=True)
	# print(totalData_df.isnull().sum())
	# print(totalData_df.shape)
	# print('-' * 40)

	totalData_df['train'] = totalData_df.apply(lambda row: month_day_jugge(row['month'], row['day']), axis=1)
	totalData_df = totalData_df.drop(columns=['month', 'day'])
	test_df['train'] = np.zeros(test_df.shape[0])

	data = pd.concat([totalData_df, test_df], ignore_index=True)
	if user and user_data:
		data['area'] = data['feed_people'].apply(lambda x: analys_user_field_by_dict(x, user_data))

	if save:
		data.to_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_2.csv'))
		print('save successfully')
	return data


def totalExposureDay_ad_train_test_B(save=False, user=False, user_data=None):
	'''
	提取已经处理好的训练集，和新测试集合并
	训练数据和广告静态数据进行merge
	:param adStatic_columns:
	:return:
	'''
	totalExposure_df = pd.read_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_2.csv')).query('train != 0.0')
	if user and user_data:
		totalExposure_df = totalExposure_df[['ad_id', 'ad_shape', 'daily_exposure', 'feed_people', 'area', 'train']]
	else:
		totalExposure_df = totalExposure_df[['ad_id', 'ad_shape', 'daily_exposure']]
	totalExposure_df['ad_id'] = totalExposure_df['ad_id'].apply(int)
	# print(totalExposure_df.isnull().sum())
	# print('-'*40)

	test_df = read_test_sample(rank_B=True)
	if user:
		test_df = test_df[
			['ad_id', 'ad_shape', 'industry_id', 'commodity_type', 'commodity_id', 'account_id', 'feed_people']]
	else:
		test_df = test_df[['ad_id', 'ad_shape', 'industry_id', 'commodity_type', 'commodity_id', 'account_id']]
	test_df['ad_id'] = test_df['ad_id'].apply(int)
	# print(test_df.isnull().sum())
	# print('-' * 40)

	test_df['train'] = np.zeros(test_df.shape[0])
	if user and user_data:
		test_df['area'] = test_df['feed_people'].apply(lambda x: analys_user_field_by_dict(x, user_data))

	data = pd.concat([totalExposure_df, test_df], ignore_index=True)
	if save:
		data.to_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_B.csv'))
	return data


def totalExposureDay_ad_processing(data_df=None, columns=['industry_id', 'commodity_type'],
                                   other_columns=None):  # , 'commodity_id', 'account_id'
	'''
	处理广告数据，columns中的变量进行one-hot编码处理，other_columns中的变量转化为int型
	:return:
	'''
	# data_df = data_df[['ad_shape', 'industry_id', 'commodity_type', 'commodity_id', 'account_id', 'daily_exposure', 'train']]
	if data_df is None:
		data_df = pd.read_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_1.csv'))
	# data_df['commodity_id'] = data_df['commodity_id'].apply(int)
	# data_df['account_id'] = data_df['account_id'].apply(int)
	if other_columns:
		for feature in other_columns:
			data_df[feature] = data_df[feature].apply(float)

	for feature in columns:
		try:
			data_df[feature] = LabelEncoder().fit_transform(data_df[feature].apply(int))
		except:
			print(feature)
			data_df[feature] = LabelEncoder().fit_transform(data_df[feature].fillna(0))
	# print(data_df.head())

	train_data = data_df.query('train != 0.0')
	train_x = train_data[['ad_id']]
	# print(any(train_x.index == train_data[['commodity_id', 'account_id']].index))
	test_data = data_df.query('train == 0.0').reset_index(drop=True)
	test_x = test_data[['ad_id']]

	enc = OneHotEncoder(sparse=False)
	for feature in columns:
		# print(feature)
		enc.fit(data_df[feature].values.reshape(-1, 1))
		train_a = enc.transform(train_data[feature].values.reshape(-1, 1))
		test_a = enc.transform(test_data[feature].values.reshape(-1, 1))

		train_a = pd.DataFrame(train_a, columns=[feature + '_%s' % i for i in range(train_a.shape[1])],
		                       index=train_x.index)
		test_a = pd.DataFrame(test_a, columns=[feature + '_%s' % i for i in range(train_a.shape[1])],
		                      index=test_x.index)
		# train_a = pd.DataFrame(train_a)
		# test_a = pd.DataFrame(test_a)
		train_x = pd.concat([train_x, train_a], axis=1).reset_index(drop=True)
		# train_x = train_x.append(train_a,axis=1)
		test_x = pd.concat([test_x, test_a], axis=1).reset_index(drop=True)
		# test_x = test_x.append(test_a,axis=1)
		# del train_a
		# del test_a
		# train_x = sparse.hstack((train_x, train_a))
		# test_x = sparse.hstack((test_x, test_a))
		print(feature + ' done')
	# print('-*-'*40)
	# print(train_x.shape)
	# print('-*-' * 40)
	if other_columns:
		train_x = pd.concat([train_x, train_data[other_columns]], axis=1)
		test_x = pd.concat([test_x, test_data[other_columns]], axis=1)
	train_x = pd.concat([train_x, train_data['train']], axis=1)
	train_x['daily_exposure'] = train_data['daily_exposure'].apply(math.log).apply(lambda x: x + 0.0001)
	# print('!!'*40)
	# print(train_x.shape)
	# print('!!'*40)

	return train_x, test_x


def totalExposureDay_ad_processing_lgb(data_df=None, columns=None, other_columns=None, rank_B=False):
	'''

	:param data_df:
	:param columns:
	:param other_columns:
	:return:
	'''
	if data_df is None:
		if not rank_B:
			data_df = pd.read_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_2.csv'))
		else:
			data_df = pd.read_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_B.csv'))
	if other_columns:
		for feature in other_columns:
			data_df[feature] = data_df[feature].apply(float)
	if columns:
		for feature in columns:
			data_df[feature] = data_df[feature].apply(int)
	train_data = data_df.query('train != 0.0')
	test_data = data_df.query('train == 0.0').reset_index(drop=True)
	if not columns is None and not other_columns is None:
		all_columns = columns + other_columns
	else:
		if columns is None:
			all_columns = other_columns
		else:
			all_columns = columns
	train_x = train_data[['ad_id'] + all_columns]
	train_x['daily_exposure'] = train_data['daily_exposure'].apply(math.log).apply(lambda x: x + 0.0001)
	train_x = pd.concat([train_x, train_data['train']], axis=1)
	test_x = test_data[['ad_id'] + all_columns]
	return train_x, test_x


def data_processing(data, totalExposure_categorical=None, totalExposuread_numerical=None, ad_categorical=None,
                    ad_numerical=None, user_categorical=None, user_numerical=None):
	'''

	:return:
	'''
	###创建字典，存储label和one_hot模型
	totalExposure_label_dict = {}
	totalExposure_hot_dict = {}
	ad_label_dict = {}
	ad_hot_dict = {}
	user_label_dict = {}
	user_hot_dict = {}

	pass


def feed_people_id_dict():
	'''
	获取各种用户定向之后的用户id的dict
	目前只做age和area的
	之后再陆续添加
	:return:
	'''
	df = pd.read_csv(os.path.join(config.file_path, 'totalExposureDay_ad_train_test_version_1.csv'))

	user_data_age = mysql.get_all_user_data_age()
	user_data_area = mysql.get_all_user_data_area()
	user_data = {}
	user_data['area'] = user_data_area
	user_data['age'] = user_data_age
	print('user get')

	feed_field = {}
	feed_area = set()
	feed_age = set()
	for i in df[['feed_people']].values:
		if i[0] == 'all':
			continue
		items = i[0].split('|')
		for item in items:
			item = item.split(':')
			if len(item) > 1:
				if item[0] == 'area':
					feed_area.add(','.join(sorted(item[1].split(','))))
				if item[0] == 'age':
					feed_age.add(','.join(sorted(item[1].split(','))))
	feed_field['area'] = feed_area
	feed_field['age'] = feed_age
	print('feed field done')

	result_dict = {}
	result_dict['area'] = {}
	result_dict['age'] = {}
	for field_name in result_dict.keys():
		for field in feed_field[field_name]:
			if field not in result_dict[field_name].keys():
				result_dict[field_name][field] = set()
			item = set(field.split(','))
			for user in user_data[field_name]:
				if len(user[1] & item) != 0:
					result_dict[field_name][field].add(int(user[0]))
	print('feed people dict done')
	return result_dict


if __name__ == '__main__':
	# user_data_to_csv()
	# totalExposureLog_to_csv()
	# totalResult_to_csv()
	# totalResult_bid_week_to_csv()
	# weekTimeResult_to_csv()
	# df = read_user_feature()
	# print(df.head())

	# time_exposure = read_all_week_time_exposure()
	# df = read_test_sample()
	# result = pd.DataFrame()
	# result['样本id'] = df['sample_id']
	# result['预估日曝光'] = df['feed_time'].apply(test_feed_time_process).astype('float32')
	# result.to_csv(os.path.join(config.file_path, 'submission.csv'), index=None)

	# time_exposure = read_all_week_time_bid_exposure()
	# df = read_test_sample()
	# result = pd.DataFrame()
	# result['样本id'] = df['sample_id']
	# exposure = []
	# for i in df[['feed_time','ad_bid']].values:
	# 	exposure.append(test_feed_time_process_bid(i[0],i[1]))
	# result['日曝光'] = np.array(exposure)
	# result.to_csv(os.path.join(config.file_path, 'submission.csv'), index=None)

	# test_baseline()

	# totalExposure_ad_user(6)

	# df = totalExposure_day_to_csv()
	# totalExposureDay_ad_user()

	# all_count = 0
	# user_data_age = mysql.get_all_user_data_age()
	# user_data_area = mysql.get_all_user_data_area()
	# user_data = {}
	# user_data['area'] = user_data_area
	# user_data['age'] = user_data_age
	# print('get user_data')
	# df = totalExposureDay_ad_train_test(save=False, user=True, user_data=None)

	# result_dict = {}
	# result_area = set()
	# result_age = set()
	# for i in df[['feed_people']].values:
	# 	if i[0] == 'all':
	# 		if 'all' in result_dict.keys():
	# 			result_dict['all'] += 1
	# 		else:
	# 			result_dict['all'] = 1
	# 		continue
	# 	items = i[0].split('|')
	# 	for item in items:
	# 		item = item.split(':')
	# 		if item[0] == 'area':
	# 			result_area.add(item[1])
	# 		if item[0] == 'age':
	# 			result_age.add(item[1])
	#
	# 		if item[0] in result_dict.keys():
	# 			result_dict[item[0]] += 1
	# 		else:
	# 			result_dict[item[0]] = 1
	# print(result_dict)
	# print(len(result_area))
	# print(len(result_age))

	result_dict = feed_people_id_dict()
	all_count = 0
	# df = totalExposureDay_ad_train_test(save=True, user=True, user_data=result_dict)
	totalExposureDay_ad_train_test_B(save=True, user=True, user_data=result_dict)