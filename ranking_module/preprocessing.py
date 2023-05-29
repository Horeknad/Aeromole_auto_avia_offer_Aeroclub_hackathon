'''
Модуль для предпроцессинга предоставляемых данных.
==================================================

Функции:
--------
convert_number_to_hour(number) `->` datatime
get_fwd_flight_time(df) `->` df
get_back_fligh_time(df) `->` df
get_difference_request_time(df) `->` df
def make_preprocess(df) `->` df

Переменные:
-----------
ndf_dictionary_city
\ndf_dictionary_airport

Зависимости:
------------
pandas

Дата:
-----
25.05.2023

Авторы:
-------
Шилова Надежда
\nЧерников Дмитрий

'''


# Необходимые библиотеки
import pandas as pd


# Загрузка словарей
name_file_dictionary = 'ranking_module\\Locations_UTC.xlsx'
xl = pd.ExcelFile(name_file_dictionary)
df_dictionary_city = xl.parse('City')
df_dictionary_airport = xl.parse('Airport')
xl.close()


def convert_number_to_hour(number):
    '''Функция конвертации числа в часы.
    ====================================

    Параметры:
    ----------
    number : float
        число, обозначащее время

    Возращаемые значения:
    ---------------------
    datetime
        время в часах и минутах

    '''
    hour = ('%.2f' % number).split(sep='.')[0]
    minute = ('%.2f' % number).split(sep='.')[1]
    return pd.to_datetime(hour+":"+minute, format='%H:%M')


def get_fwd_flight_time(df, all_columns):
    '''Функция для вычисления времени полёта туда.
    ==============================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя
    all_columns : list
        список с названиями всех столбцов

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с новыми полями FwdFlightTime, FwdFrom, FwdTo

    '''
    # Выделение путей следования
    df['FwdFrom'] = df['SearchRoute'].str.split('/').str[0].str[0:3]
    df['FwdTo'] = df['SearchRoute'].str.split('/').str[0].str[3:]

    # Получение часовой зоны пути туда
    df = df.merge(
        df_dictionary_city[['IataCode','TimeZone']], 
        left_on='FwdFrom',
        right_on='IataCode',
        how='left',
        indicator=True
    )
    df.rename(columns={'TimeZone': 'TimeZoneFwdFrom'}, inplace=True)
    df.drop(['IataCode', '_merge'], axis=1, inplace=True)


    # Поиск часовой зоны по аэропортам
    df_for_airoport = df[~(df['TimeZoneFwdFrom'].notna())].copy()
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_airport[['IATACode','CityID']],
        left_on='FwdFrom', 
        right_on='IATACode',
        how='left',
        indicator=True
    )
    df_for_airoport.drop('_merge', axis=1, inplace=True)
    df_for_airoport_all = df_for_airoport.merge(
        df_dictionary_city[['Id','TimeZone']],
        left_on='CityID',
        right_on='Id',
        how='left',
        indicator=True
    )
    df_for_airoport_all.drop(
        ['TimeZoneFwdFrom', 'IATACode', 'CityID', 'Id', '_merge'],
        axis=1,
        inplace=True
    )
    df_for_airoport_all.rename(columns={'TimeZone': 'TimeZoneFwdFrom'}, inplace=True)

    # Объединение часовых зон из справочников аэропортов и городов
    column_for_merge = all_columns + ['FwdFrom', 'FwdTo']
    df = df.merge(
        df_for_airoport_all,
        left_on=column_for_merge,
        right_on=column_for_merge,
        how="left",
        indicator='exists'
    )
    df['TimeZoneFwdFrom'] = df['TimeZoneFwdFrom_x'].fillna(0) + df['TimeZoneFwdFrom_y'].fillna(0)
    df.drop(['TimeZoneFwdFrom_x', 'TimeZoneFwdFrom_y', 'exists'], axis=1, inplace=True)
    
    # Часовой пояс туда
    df = df.merge(
        df_dictionary_city[['IataCode','TimeZone']], 
        left_on='FwdTo',
        right_on='IataCode',
        how='left',
        indicator=True
    )
    df.rename(columns={'TimeZone': 'TimeZoneFwdTo'}, inplace=True)
    df.drop(['IataCode', '_merge'], axis=1, inplace=True)

    # Поиск часовой зоны по аэропортам
    df_for_airoport = df[~(df['TimeZoneFwdTo'].notna())]
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_airport[['IATACode','CityID']],
        left_on='FwdTo', 
        right_on='IATACode',
        how='left',
        indicator=True
    )
    df_for_airoport.drop('_merge', axis=1, inplace=True)
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_city[['Id','TimeZone']],
        left_on='CityID',
        right_on='Id',
        how='left',
        indicator=True
    )
    df_for_airoport.drop(['TimeZoneFwdTo', 'IATACode', 'CityID', 'Id', '_merge'], axis=1, inplace=True)
    df_for_airoport.rename(columns={'TimeZone': 'TimeZoneFwdTo'}, inplace=True)
    
    # Объединение данных из справочника аэропортов и городов
    column_for_merge += ['TimeZoneFwdFrom']
    df = df.merge(
        df_for_airoport,
        left_on=column_for_merge,
        right_on=column_for_merge,
        how="left"
    )
    df['TimeZoneFwdTo'] = df['TimeZoneFwdTo_x'].fillna(0) + df['TimeZoneFwdTo_y'].fillna(0)
    df.drop(['TimeZoneFwdTo_x', 'TimeZoneFwdTo_y'], axis=1, inplace=True)

    # Разница во времени и часовых поясах
    df['DifferenceDate'] = df[
        'ArrivalDate'].astype("datetime64[ns]") - df['DepartureDate'].astype("datetime64[ns]")
    df['DifferenceZone'] = df.apply(lambda x: x['TimeZoneFwdFrom'] - x['TimeZoneFwdTo'], axis=1)
    df.loc[df['DifferenceZone'] < 0, 'DifferenceZoneSymbol'] = '-'
    df.loc[df['DifferenceZone'] >= 0, 'DifferenceZoneSymbol'] = '+'
    df = df[df['DifferenceDate'].notna()]
    df['DifferenceZone'] = df['DifferenceZone'].abs().apply(convert_number_to_hour).dt.strftime('%H:%M:%S')
    
    # Вычисление времени перелёта
    df.loc[df['DifferenceZoneSymbol'] == '-', 'FwdFlightTime'] =df.apply(
        lambda x: x['DifferenceDate'] - x['DifferenceZone'], axis=1
    )
    df.loc[df['DifferenceZoneSymbol'] == '+', 'FwdFlightTime'] = df.apply(
        lambda x: x['DifferenceDate'] + x['DifferenceZone'], axis=1
    )

    # Удаление лишних столбцов
    df.drop(
        ['TimeZoneFwdFrom', 'TimeZoneFwdTo', 'DifferenceDate', 'DifferenceZone', 'DifferenceZoneSymbol'],
        axis=1,
        inplace=True
    )
    
    # Время полёта
    df['FwdFlightTime'] = df['FwdFlightTime'].values.astype('<m8[m]').astype(int)
    df = df.drop_duplicates()

    return df


def get_back_fligh_time(df, all_columns):
    '''Функция для вычисления времени полёта обратно.
    =================================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя
    all_columns : list
        список с названиями всех столбцов

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с новым полем BackFlightTime, BackTo, BackFrom

    '''
    # Выделение путей следования
    df['BackFrom'] = df['SearchRoute'].str.split('/').str[1].str[0:3]
    df['BackTo'] = df['SearchRoute'].str.split('/').str[1].str[3:]

    # Отсекаем пустые значения
    df = df[df['BackFrom'].notna()]
    df = df[df['ReturnDepatrureDate'].notna()]

    # Часовой пояс туда
    df = df.merge(
        df_dictionary_city[['IataCode','TimeZone']],
        left_on='BackFrom',
        right_on='IataCode',
        how='left',
        indicator=True
    )
    df.rename(columns={'TimeZone': 'TimeZoneBackFrom'}, inplace=True)
    df.drop(['IataCode', '_merge'], axis=1, inplace=True)

    # Поиск часовой зоны по аэропортам
    df_for_airoport = df[~(df['TimeZoneBackFrom'].notna())]
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_airport[['IATACode','CityID']],
        left_on='BackFrom',
        right_on='IATACode',
        how='left',
        indicator=True
    )
    df_for_airoport.drop('_merge', axis=1, inplace=True)
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_city[['Id','TimeZone']],
        left_on='CityID',
        right_on='Id',
        how='left',
        indicator=True
    )
    df_for_airoport.drop(['TimeZoneBackFrom', 'IATACode', 'CityID', 'Id', '_merge'], axis=1, inplace=True)
    df_for_airoport.rename(columns={'TimeZone': 'TimeZoneBackFrom'}, inplace=True)

    # Объединение данных из справочника аэропортов и городов
    column_for_merge = all_columns + ['FwdTo', 'FwdFrom', 'BackFrom', 'BackTo', 'FwdFlightTime']
    df = df.merge(
        df_for_airoport,
        left_on=column_for_merge,
        right_on=column_for_merge,
        how="left"
    )
    df['TimeZoneBackFrom'] = df['TimeZoneBackFrom_x'].fillna(0) + df['TimeZoneBackFrom_y'].fillna(0)
    df.drop(['TimeZoneBackFrom_x', 'TimeZoneBackFrom_y'], axis=1, inplace=True)

    # Часовой пояс обратно
    df = df.merge(
        df_dictionary_city[['IataCode','TimeZone']],
        left_on='BackTo',
        right_on='IataCode',
        how='left',
        indicator=True
    )
    df.rename(columns={'TimeZone': 'TimeZoneBackTo'}, inplace=True)
    df.drop(['IataCode', '_merge'], axis=1, inplace=True)

    # Поиск часовой зоны по аэропортам
    df_for_airoport = df[~(df['TimeZoneBackTo'].notna())]
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_airport[['IATACode','CityID']],
        left_on='BackTo',
        right_on='IATACode',
        how='left',
        indicator=True
    )
    df_for_airoport.drop('_merge', axis=1, inplace=True)
    df_for_airoport = df_for_airoport.merge(
        df_dictionary_city[['Id','TimeZone']],
        left_on='CityID',
        right_on='Id',
        how='left',
        indicator=True
    )
    df_for_airoport.drop(['TimeZoneBackTo', 'IATACode', 'CityID', 'Id', '_merge'], axis=1, inplace=True)
    df_for_airoport.rename(columns={'TimeZone': 'TimeZoneBackTo'}, inplace=True)

    # Объединение данных из справочника аэропортов и городов
    column_for_merge += ['TimeZoneBackTo']
    df = df.merge(
        df_for_airoport,
        left_on=column_for_merge,
        right_on=column_for_merge,
        how="left"
    )
    df['TimeZoneBackFrom'] = df['TimeZoneBackFrom_x'].fillna(0) + df['TimeZoneBackFrom_y'].fillna(0)
    df.drop(['TimeZoneBackFrom_x', 'TimeZoneBackFrom_y'], axis=1, inplace=True)

    # Разница во времени и часовых поясах
    df['DifferenceDate'] = df['ReturnArrivalDate'].astype(
        "datetime64[ns]") - df['ReturnDepatrureDate'].astype("datetime64[ns]")
    df['DifferenceZone'] = df.apply(lambda x: x['TimeZoneBackFrom'] - x['TimeZoneBackTo'], axis=1)
    df.loc[df['DifferenceZone'] < 0, 'DifferenceZoneSymbol'] = '-'
    df.loc[df['DifferenceZone'] >= 0, 'DifferenceZoneSymbol'] = '+'
    df['DifferenceZone'] = df['DifferenceZone'].abs().apply(convert_number_to_hour).dt.strftime('%H:%M:%S')

    # Вычисление времени перелёта
    df.loc[df['DifferenceZoneSymbol'] == '-', 'BackFlightTime'] = df.apply(
        lambda x: x['DifferenceDate'] - x['DifferenceZone'], axis=1)
    df.loc[df['DifferenceZoneSymbol'] == '+', 'BackFlightTime'] = df.apply(
        lambda x: x['DifferenceDate'] + x['DifferenceZone'], axis=1)
    
    # Удаление лишних столбцов
    df.drop(
        [
            'TimeZoneBackFrom',
            'TimeZoneBackTo',
            'DifferenceDate',
            'DifferenceZone',
            'DifferenceZoneSymbol'
        ],
        axis=1,
        inplace=True
    )

    # Время полёта
    df['BackFlightTime'] = df['BackFlightTime'].values.astype('<m8[m]').astype(int)
    df = df.drop_duplicates()

    return df


def get_difference_request_time(df):
    '''Функция для вычисления разницы от запрошенной даты и времени пользователем и выданным временем.
    ==================================================================================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с новыми полями FwdDepDelta и BackDepDelta

    '''
    # Отсечение запросов без времени
    df_with_time = df.loc[~(df['RequestDepartureDate'].str.contains('00:00:00.000', na=False))]

    df_with_time['RequestDepartureDate'] = df_with_time['RequestDepartureDate'].astype("datetime64[ns]")
    df_with_time['DepartureDate'] = df_with_time['DepartureDate'].astype("datetime64[ns]")

    # Нахождение разницы
    df_with_time.loc[
        df_with_time['RequestDepartureDate'] >= df_with_time['DepartureDate'], 'FwdDepDelta'
        ] = df_with_time.apply(lambda x: x['RequestDepartureDate'] - x['DepartureDate'], axis=1)
    df_with_time.loc[
        df_with_time['RequestDepartureDate'] <= df_with_time['DepartureDate'], 'FwdDepDelta'
        ] = df_with_time.apply(lambda x: x['DepartureDate'] - x['RequestDepartureDate'], axis=1)
    
    # Перевод в минуты
    df_with_time['FwdDepDelta'] = df_with_time['FwdDepDelta'].values.astype('<m8[m]').astype(int)

    # Объединение
    df = df.merge(df_with_time[['FwdDepDelta']], left_index=True, right_index=True, how='left')


    # Отсечение пустых запросов
    df_without_nan = df.loc[df['RequestReturnDate'].notna()]

    # Отсечение запросов без времени
    df_with_time = df_without_nan.loc[~(df_without_nan['RequestReturnDate'].str.contains('00:00:00.000', na=False))]

    df_with_time['RequestReturnDate'] = df_with_time['RequestReturnDate'].astype("datetime64[ns]")
    df_with_time['ReturnDepatrureDate'] = df_with_time['ReturnDepatrureDate'].astype("datetime64[ns]")

    # Нахождение разницы
    df_with_time.loc[
        df_with_time['RequestReturnDate'] >= df_with_time['ReturnDepatrureDate'], 'BackDepDelta'
        ] = df_with_time.apply(lambda x: x['RequestReturnDate'] - x['ReturnDepatrureDate'], axis=1)
    df_with_time.loc[
        df_with_time['RequestReturnDate'] <= df_with_time['ReturnDepatrureDate'], 'BackDepDelta'
        ] = df_with_time.apply(lambda x: x['ReturnDepatrureDate'] - x['RequestReturnDate'], axis=1)
    
    # Убираем пустые значения
    df_with_time = df_with_time[df_with_time['BackDepDelta'].notna()]

    # Перевод в минуты
    df_with_time['BackDepDelta'] = df_with_time['BackDepDelta'].values.astype('<m8[m]').astype(int)

    # Объединение
    df = df.merge(df_with_time[['BackDepDelta']], left_index=True, right_index=True, how='left')

    return df


def get_days_before_departure(df):
    '''Функция для вычисления разницы от дня покупки до дня вылета.
    ===============================================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с новыми полями RequestDelta

    '''
    # Отсечение запросов без времени

    df['RequestDepartureDate'] = df['RequestDepartureDate'].astype("datetime64[ns]")
    df['RequestDate'] = df['RequestDate'].astype("datetime64[ns]")

    # Нахождение разницы
    df.loc[
        df['RequestDepartureDate'] >= df['RequestDate'], 'RequestDelta'
        ] = df.apply(lambda x: x['RequestDepartureDate'] - x['RequestDate'], axis=1)
    
    # Перевод в сутки
    df['RequestDelta'] = df['RequestDelta'].values.astype('<m8[m]').astype(int) / 1140
    df = df.round({'RequestDelta': 0})

    return df



def make_preprocess(df):
    '''Функция добавляет все необходимые столбцы для предсказания в модели.
    =======================================================================

    Параметры:
    ----------
    df : DataFrame
        данные запроса и выдачи пользователя

    Возвращаемые значения:
    ----------------------
    df : DataFrame
        с добавлением новых столбцов

    '''
    # Выделение столбцов
    all_columns = [
        'ID', 'RequestID', 'EmployeeId', 'RequestDate', 'ClientID', 'ValueRu', 
        'RequestDepartureDate', 'RequestReturnDate', 'FligtOption', 'DepartureDate',
        'ArrivalDate', 'ReturnDepatrureDate', 'ReturnDepatrureDate', 'SegmentCount',
        'Amount', 'class', 'IsBaggage', 'isRefundPermitted', 'isRefundPermitted', 'isDiscount',
        'InTravelPolicy', 'ReturnArrivalDate', 'isExchangePermitted', 'SearchRoute'
    ]
    datetime_features = [
        'RequestDate',
        'RequestDepartureDate',
        'RequestReturnDate',
        'DepartureDate',
        'ArrivalDate',
        'ReturnDepatrureDate',
        'ReturnArrivalDate'
    ]
    numerical_features = [
        'SegmentCount',
        'Amount',
        'IsBaggage',
        'isRefundPermitted',
        'isExchangePermitted',
        'isDiscount',
        'InTravelPolicy'
    ]

    df_fwd_flight_time = get_fwd_flight_time(df, all_columns)
    all_columns = all_columns + ['FwdFrom', 'FwdTo']
    df = df.merge(
        df_fwd_flight_time,
        left_on=all_columns,
        right_on=all_columns,
        how="left"
    )

    df_back_fligh_time = get_back_fligh_time(df, all_columns)
    all_columns = all_columns + ['FwdFlightTime', 'BackFrom', 'BackTo']
    df = df.merge(
        df_back_fligh_time,
        left_on=all_columns,
        right_on=all_columns,
        how="left"
    )
    
    df = get_difference_request_time(df)
    df = get_days_before_departure(df)

    # Время между перелётом туда и обратно
    df['FlightTimeTotal'] = df['FwdFlightTime'].fillna(0.0) + df['BackFlightTime'].fillna(0.0)

    df = df.reset_index()
    df_request = df['RequestID'].copy()
    df = df.set_index('RequestID')

    # Разница от минимальной стоимости
    df['DeltaAmount'] = df.groupby(level=0)['Amount'].min()
    df['DeltaAmount'] -= df['Amount']
    df['DeltaAmount'] = df['DeltaAmount'].abs()

    # Разница от быстрого времени
    df['DeltaFlightTime'] = df.groupby(level=0)['FlightTimeTotal'].min()
    df['DeltaFlightTime'] -= df['FlightTimeTotal']
    df['DeltaFlightTime'] = df['DeltaFlightTime'].abs()

    df = df.set_index('index')
    df['RequestID'] = df_request

    return df