import pandas as pd
import glob
import os


def refine_bss_data():
    location = get_location()

    ebirds = process_ebirds_data(location)
    naturing = process_naturing_data(location)

    combined_data = pd.concat([ebirds, naturing])
    # combined_data.to_excel(r'C:\Users\z\Desktop\output.xlsx', index=False)
    # 같은 파일이 이미 존재할 경우 어떻게 처리되는지 체크
    print(combined_data)


def process_ebirds_data(location):
    ebirds_raw_data_list = get_ebirds_raw_data_list()
    ebirds_trimmed_concat_data = trim_ebirds_raw_data(ebirds_raw_data_list, location)
    return ebirds_trimmed_concat_data


def process_naturing_data(location):
    naturing_raw_data = get_naturing_raw_data()
    naturing_trimmed_data = trim_naturing_raw_data(naturing_raw_data, location)
    return naturing_trimmed_data


def get_location():
    # delete later
    return '어린이대공원'

    location = input('탐조 장소를 입력해주세요.\n')

    while location not in ['여의샛강', '어린이대공원', '북서울꿈의숲', '창경궁', '남산', '올림픽공원', '푸른수목원', '강서습지', '중랑천']:
        print('다음 중 탐조 장소를 입력해주세요. 여의샛강, 어린이대공원, 북서울꿈의숲, 창경궁, 남산, 올림픽공원, 푸른수목원, 강서습지, 중랑천\n')
        location = input()

    return location


def get_ebirds_raw_data_list():
    ebirds_raw_data_list = []

    # delete later
    path = r'C:\Users\z\Desktop\서울의새\데이터분석\2022-2023\raw_data\02_어린이대공원\ebird_kr'
    #path = input('ebirds 로우 파일들이 들어있는 폴더를 알려주세요\n')
    files_path = glob.glob(os.path.join(path, "*.csv"))
    for file_path in files_path:
        df = pd.read_csv(file_path)
        check_file_is_ebird_raw(df)
        ebirds_raw_data_list.append(df)

    if len(ebirds_raw_data_list) == 0:
        raise ValueError('이버드 로우 파일을 가져오지 못했습니다. 올바른 경로인지 확인해주세요.')

    return ebirds_raw_data_list


def check_file_is_ebird_raw(df):
    if not {'Observation date', 'Species', 'Count'}.issubset(df.columns):
        raise ValueError('이버드 로우 파일 형식이 아닙니다. 확인 후 올바른 파일을 넣어주세요.')
    return


def trim_ebirds_raw_data(data_list, location):
    temp_ebirds_trimmed_data_list = []

    for data in data_list:
        data = get_necessary_columns_in_ebird_raw_data(data)
        data = split_observation_date(data)
        data = add_location_is_naturing_columns(data, location, False)
        temp_ebirds_trimmed_data_list.append(data)

    ebirds_trimmed_concat_data = pd.concat(temp_ebirds_trimmed_data_list, axis=0, ignore_index=True)
    return ebirds_trimmed_concat_data


def get_necessary_columns_in_ebird_raw_data(df):
    return df[['Species', 'Count', 'Observation date']]


def split_observation_date(df):
    df['Observation date'] = pd.to_datetime(df['Observation date'])
    df['Year'] = df['Observation date'].dt.year
    df['Month'] = df['Observation date'].dt.month
    df['Day'] = df['Observation date'].dt.day
    df = df.drop('Observation date', axis=1)
    return df


def add_location_is_naturing_columns(df, location, is_naturing):
    df['Location'] = location
    df['isNaturing'] = is_naturing
    return df


def get_naturing_raw_data():
    pd.set_option('display.max_columns', None)
    #path = input('네이처링 로우 파일의 위치를 알려주세요.\n')

    # delete later
    path = r'C:\Users\z\Desktop\서울의새\데이터분석\2022-2023\raw_data\02_어린이대공원\naturing\네이처링 어대공_20230514183119.csv'
    naturing_raw_data = pd.read_csv(path, encoding='cp949')
    check_file_is_naturing_raw(naturing_raw_data)

    return naturing_raw_data


def check_file_is_naturing_raw(df):
    if not {'관찰일', '생물이름', '생물분류'}.issubset(df.columns):
        raise ValueError('네이처링 로우 파일 형식이 아닙니다. 확인 후 올바른 파일을 넣어주세요.')
    return


def trim_naturing_raw_data(data, location):
    df = drop_not_bird_row(data)
    df = drop_out_dated_row(df)
    df = drop_invalid_location_row(df, location)

    df = get_necessary_columns_in_naturing_raw_data(df)
    df = trim_date_to_year_month_day(df)
    df = insert_required_column(df, location)

    return df


def drop_not_bird_row(data):
    data = data[data['생물분류'] == '조류']
    data = data.drop('생물분류', axis=1)
    return data


def drop_out_dated_row(data):
    data['관찰일'] = pd.to_datetime(data['관찰일'])
    return data[(data['관찰일'] > '2022-04-01') & (data['관찰일'] < '2023-03-01')]


def drop_invalid_location_row(data, location):
    # 추후 사이트별 위도/경도 값 딕셔너리 구축 필요
    # 어린이대공원의 경우 경도 127.069425-127.092399 위도 37.541118-37.559150

    data = drop_invalid_location_row_with_longitude(data, location)
    data = drop_invalid_location_row_with_latitude(data, location)

    return data


def drop_invalid_location_row_with_longitude(data, location):
    data['경도'] = pd.to_numeric(data['경도'])
    data = data[(127.069425 < data['경도']) & (data['경도'] < 127.092399)]

    return data


def drop_invalid_location_row_with_latitude(data, location):
    data['위도'] = pd.to_numeric(data['위도'])
    data = data[(37.541118 < data['위도']) & (data['위도'] < 37.559150)]

    return data


def get_necessary_columns_in_naturing_raw_data(data):
    data = data[['관찰일', '생물이름']]
    return data.rename(columns={'생물이름': 'Species'})


def trim_date_to_year_month_day(data):
    data['Year'] = data['관찰일'].dt.year
    data['Month'] = data['관찰일'].dt.month
    data['Day'] = data['관찰일'].dt.day
    data = data.drop('관찰일', axis=1)

    return data


def insert_required_column(data, location):
    data['Location'] = location
    data['isNaturing'] = True
    data['Count'] = 0

    return data


if __name__ == '__main__':
    refine_bss_data()





