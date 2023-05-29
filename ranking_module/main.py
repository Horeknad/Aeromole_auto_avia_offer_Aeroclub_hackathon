'''
Модуль для предсказания ранжирования предложений.
=================================================

Функции:
--------
make_preds(df, model) `->` df

Переменные:
-----------
name_file_dictionary `->` путь до словаря городов и аэропортов в формате xlsx
name_model `->` название ml модели

Зависимости:
------------
catboost
\njoblib
\nlogging
\nnumpy
\npandas
\npathlib
\nsys

Дата:
-----
25.05.2023

Авторы:
-------
Шилова Надежда
\nЧерников Дмитрий

'''


# Необходимые библиотеки
import catboost
import joblib
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sys
## Внутренние файлы
from preprocessing import make_preprocess


# Переменные
name_model = 'ranking_module\\ranking_model_catb_2500_cw1.5_0.84.pkl'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Загрузка модели
model = joblib.load(name_model)

def make_preds(df, model):
    '''Функция делает ранжирование по представляемым данным.
    ========================================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя
    model : CatBoostClassifier
        обученная модель

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с заполненным рейтингом ранжирования Position

    '''

    numerical_features = [
        'FwdFlightTime',
        'BackFlightTime',
        'FwdDepDelta',
        'BackDepDelta',
        'RequestDelta',
        'SegmentCount',
        'Amount',
        'IsBaggage',
        'isRefundPermitted',
        'isExchangePermitted',
        'isDiscount',
        'InTravelPolicy',
        'FlightTimeTotal',
        'DeltaAmount',
        'DeltaFlightTime',
    ]

    categorical_features = [
        'FwdFrom', 'FwdTo',
        'BackFrom', 'BackTo',
    ]

    used_features = numerical_features + categorical_features

    # Подготовка данных
    X = df[used_features].copy()
    X.loc[:,categorical_features] = X.loc[:,categorical_features].fillna('')

    # Предсказание
    proba = model.predict_proba(X)

    # Постпроцессинг, определение ранжирования
    df_rating = df[['ID', 'RequestID']].copy()
    df_rating['SentOption'] = pd.Series(proba[:,1], index=df.index)

    df_rating = df_rating.reset_index() \
        .sort_values(by=['RequestID', 'SentOption'], ascending=False) \
        .copy()

    df_rating['Position'] = pd.Series(np.arange(1,len(df_rating)+1), df_rating.index)

    df_rating = df_rating.set_index('RequestID')
    df_rating['Start'] = df_rating.groupby(level=0)['Position'].min()
    df_rating['Position'] -= df_rating['Start']

    position = df_rating.set_index('index') \
        .drop(columns=['SentOption', 'Start']) \
        .sort_index() \
        .reset_index(drop=True)
    position += 1

    # Добавление в результат
    df_result = df.copy()
    df_result['Position'] = position['Position']

    return df_result.copy()


if __name__ == "__main__":
    while True:
        # Загрузка файла
        file_request = input("Введите путь/название файла в формате xlsx: ")
        df_agent = pd.read_excel(file_request)
        logger.info(f'Input dataset: {df_agent.shape}')
        df_agent.drop(['Position ( from 1 to n)'], axis=1, inplace=True)

        processed_df = make_preprocess(df_agent)
        logger.info(f'Number of features: {processed_df.shape[1]}')

        df_result = make_preds(processed_df, model)

        # Выгрузка файла
        name_file = Path(file_request).stem
        result_path_file = file_request.replace(name_file, f'{name_file}Result')
        df_result.to_excel(result_path_file)
        logger.info(f'Predictions done: {df_result}')
        print(f'Работа выполнена, ваш файл: {result_path_file}')
