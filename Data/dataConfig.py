# coding:utf-8
import os

current_path = os.path.realpath(__file__)
father_path = os.path.dirname(os.path.dirname(current_path))


class dataConfig:
	def __init__(self):
		'''
		file_path:数据存储地址

		'''
		self.file_path = os.path.join(father_path, 'Data/data')
		self.data_name = ['ad_operation.dat', 'ad_static_feature.out', 'totalExposureLog.out', 'user_data',
		                  'test_sample.dat']
		self.save_csv_name = 'totalExposureLog_%s'
		self.totalExposureLog_columns = ['ad_requests_id', 'ad_requests_time', 'ad_location_id', 'user_id', 'ad_id',
		                                 'ad_shape', 'ad_bid', 'ad_pctr', 'ad_quality_ecpm', 'ad_total_ecpm']
		self.ad_operation_columns = ['ad_id','create/change_time','operation_type','change_field','field_after_change']
		self.ad_static_feature_columns = ['ad_id','create_time','account_id','commodity_id','commodity_type','industry_id','source_type']
		self.user_feature_columns = ['user_id','age','gender','area','status','education','consuption_ability','device','work','connection_type','behavior']
