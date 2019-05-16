#coding:utf-8
import sys
sys.path.append('/Users/ruicheng/Documents/上海师范研究生/腾讯2019广告算法大赛/TencentCTR')
import numpy as np
import math
import os
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from Data.dataProcessing import totalExposureDay_ad_train_test, totalExposureDay_ad_processing_lgb, read_test_sample, totalExposure_day_ad_dict
from Data.dataConfig import dataConfig

dataConfig = dataConfig()
ad_exposure_dict = totalExposure_day_ad_dict()
is_training = True
categorical_feature = ['industry_id', 'commodity_type', 'ad_shape', 'commodity_id', 'account_id']
numerical_feature = ['area']

# df = totalExposureDay_ad_train_test()
pd.set_option('display.width',None)
# print(df.head())

data_x, data_test = totalExposureDay_ad_processing_lgb(columns=categorical_feature,other_columns=numerical_feature)
test = read_test_sample()
test['rank'] = test[['ad_id', 'ad_bid']].groupby('ad_id')['ad_bid'].apply(lambda row:row.rank()).apply(int)
test_df = test[['sample_id', 'ad_id', 'rank']]

###分割训练集和测试集
data_x = data_x.iloc[:, 1:]
data_x_train = data_x.query('train == 1.0').drop(columns=['train'])
data_x_test = data_x.query('train == 2.0').drop(columns=['train'])
train_y = data_x_train['daily_exposure']
train_x = data_x_train.drop(columns=['daily_exposure'])
test_y = data_x_test['daily_exposure']
test_x = data_x_test.drop(columns=['daily_exposure'])
train_data = lgb.Dataset(train_x, label=train_y, feature_name=categorical_feature + numerical_feature,
                   categorical_feature=categorical_feature)
test_data = lgb.Dataset(test_x, label=test_y, feature_name=categorical_feature + numerical_feature,
                   categorical_feature=categorical_feature)

data_test_index = data_test['ad_id'].apply(int)
data_test_data = data_test.iloc[:, 1:]

###模型训练
lgb_params = {
    'boosting_type': 'gbdt',
    'objective': 'regression', #xentlambda
    'metric': 'mse',
    'silent':0,
    'learning_rate': 0.1,
    'num_leaves': 200,  # we should let it be smaller than 2^(max_depth)
    'max_depth': -1,  # -1 means no limit
    'min_child_samples': 15,  # Minimum number of data need in a child(min_data_in_leaf)
    'max_bin': 200,  # Number of bucketed bin for feature values
    'subsample': 0.8,  # Subsample ratio of the training instance.
    'subsample_freq': 1,  # frequence of subsample, <=0 means no enable
    'colsample_bytree': 0.5,  # Subsample ratio of columns when constructing each tree.
    'min_child_weight': 0,  # Minimum sum of instance weight(hessian) needed in a child(leaf)
    #'scale_pos_weight':100,
    'subsample_for_bin': 200000,  # Number of samples for constructing bin
    'min_split_gain': 0,  # lambda_l1, lambda_l2 and min_gain_to_split to regularization
    'reg_alpha': 1.5,  # L1 regularization term on weights
    'reg_lambda': 1.5,  # L2 regularization term on weights
    'nthread': 10,
    'verbose': 0,
}
if is_training:
    print('train start')
    # model.fit(train_x,train_y)
    model = lgb.train(lgb_params, train_data, 2000, valid_sets=[test_data])
    model.save_model('/Users/ruicheng/Documents/上海师范研究生/腾讯2019广告算法大赛/TencentCTR/Lightgbm/model/lightgbm.txt')
    print('train finish')

###验证模型效果
pred = model.predict(test_x)
print(mean_squared_error(test_y,pred))

###输出结果
def ad_exposure_get(ad_id,ad_exposure):
    '''
    历史广告id用历史数据填充
    :param ad_id:
    :param ad_exposure:
    :return:
    '''
    ad_id = int(ad_id)
    if ad_id in ad_exposure_dict.keys():
        return round(ad_exposure_dict[ad_id], 4)
    else:
        return ad_exposure

data_test_y = model.predict(data_test_data)
temp_df = pd.DataFrame()
temp_df['ad_id'] = data_test_index
temp_df['exposure'] = data_test_y
temp_df['exposure'] = temp_df['exposure'].apply(math.exp).round(4)
###历史广告用历史数据填充，新广告用模型填充
temp_df['exposure'] = temp_df.apply(lambda row:ad_exposure_get(row['ad_id'], row['exposure']), axis=1)

ad_exposure = {}
for i in temp_df.values:
    ad_id = int(i[0])
    exposure = round(i[1], 4)
    ad_exposure[ad_id] = exposure

###单调性
results = []
for item in test_df[['ad_id','rank']].values:
    result = round(ad_exposure[int(item[0])] + (int(item[1]) / 6), 4)
    results.append(result)

test_df['exposure'] = np.array(results)
print(test_df.head())
test_df.set_index('sample_id')[['exposure']].to_csv(os.path.join(dataConfig.file_path, 'submission.csv'), header=None)
print('save successfully')