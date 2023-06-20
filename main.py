import pandas as pd
import glob
import os


def refine_bss_data():
    location = get_location()

    ebirds, birds_name_table = process_ebirds_data(location)
    naturing = process_naturing_data(location, birds_name_table)

    combined_data = pd.concat([ebirds, naturing])
    combined_data = combined_data[
        ['Species', 'English name', 'Korean name', 'Count', 'Year', 'Month', 'Day', 'Location', 'isNaturing']]

    # rename function name later
    combined_data = merge_naturing_to_ebirds(combined_data)

    # use only needed
    # ------------------------------------------------------------------
    # species_num_per_month = combined_data.groupby(['Year', 'Month']).size().reset_index()
    # species_num_per_month['Location'] = location
    # species_num_per_month.columns = ['Year', 'Month', 'Species count', 'Location']
    # species_num_per_month.to_excel(r'C:\Users\z\Desktop\23output.xlsx', index=False)
    # ------------------------------------------------------------------

    combined_data.to_excel(r'C:\Users\z\Desktop\output.xlsx', index=False)
    # 같은 파일이 이미 존재할 경우 어떻게 처리되는지 체크
    # print(combined_data)


def process_ebirds_data(location):
    ebirds_kr_trimmed_concat_data = trim_ebirds_raw_data(location, lang='kr')
    ebirds_en_trimmed_concat_data = trim_ebirds_raw_data(location, lang='en')

    ebirds_final_data = concat_ebirds_kr_with_en_data(kr_data=ebirds_kr_trimmed_concat_data,
                                                      en_data=ebirds_en_trimmed_concat_data)
    # use only needed
    # ------------------------------------------------------------------
    ebirds_final_data = handle_courses_on_single_day(ebirds_final_data)
    ebirds_final_data = handle_record_on_single_month(ebirds_final_data)
    # ------------------------------------------------------------------

    birds_name_table = set_and_return_birds_name_table(ebirds_final_data)
    return ebirds_final_data, birds_name_table


def process_naturing_data(location, birds_name_table):
    naturing_raw_data = get_naturing_raw_data()
    naturing_trimmed_data = trim_naturing_raw_data(naturing_raw_data, location, birds_name_table)

    # 집비둘기는 세지 않음~
    naturing_trimmed_data.drop(naturing_trimmed_data[naturing_trimmed_data['Korean name'].astype('string') == '집비둘기'].index, inplace=True)
    return naturing_trimmed_data


def get_location():
    # delete later
    return '올림픽공원'

    location = input('탐조 장소를 입력해주세요.\n')

    while location not in ['여의샛강', '어린이대공원', '북서울꿈의숲', '창경궁', '남산', '올림픽공원', '푸른수목원', '강서습지', '중랑천']:
        print('다음 중 탐조 장소를 입력해주세요. 여의샛강, 어린이대공원, 북서울꿈의숲, 창경궁, 남산, 올림픽공원, 푸른수목원, 강서습지, 중랑천\n')
        location = input()

    return location


def get_ebirds_raw_data_list(lang='kr'):
    ebirds_raw_data_list = []
    path = ''

    # delete later
    if lang == 'kr':
        path = r'C:\Users\z\Desktop\서울의새\데이터분석\2022-2023\raw_data\06_올림픽공원\ebird_kr'
    elif lang == 'en':
        path = r'C:\Users\z\Desktop\서울의새\데이터분석\2022-2023\raw_data\06_올림픽공원\ebird_en'

    # if lang == 'kr':
    #     path = input('ebirds kr raw 파일들이 들어있는 폴더를 알려주세요\n')
    # elif lang == 'en':
    #     path = input('ebirds en raw 파일들이 들어있는 폴더를 알려주세요\n')
    # else:
    #     raise ValueError('코드 오류: get_ebirds_raw_data_list param error')

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


def trim_ebirds_raw_data(location, lang='kr'):
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    # delete later - for debugging

    data_list = get_ebirds_raw_data_list(lang)

    temp_ebirds_trimmed_data_list = []

    for data in data_list:
        data = get_necessary_columns_in_ebird_raw_data(data)
        separate_scientific_name_from_species_column(data, lang)
        data = split_observation_date(data)
        data = add_location_and_is_naturing_columns(data, location, False)
        temp_ebirds_trimmed_data_list.append(data)

    ebirds_trimmed_concat_data = pd.concat(temp_ebirds_trimmed_data_list, axis=0, ignore_index=True)
    return ebirds_trimmed_concat_data


def concat_ebirds_kr_with_en_data(kr_data, en_data):
    kr_data['English name'] = en_data['English name']
    # TODO 추후수정 요. 그냥 컬럼 떼다 붙이는게 아니라 birds_name_table 에서 참조 후 붙여야함

    return kr_data


def get_necessary_columns_in_ebird_raw_data(df):
    return df[['Species', 'Count', 'Observation date']]


def split_observation_date(df):
    df['Observation date'] = pd.to_datetime(df['Observation date'])
    df['Year'] = df['Observation date'].dt.year
    df['Month'] = df['Observation date'].dt.month
    df['Day'] = df['Observation date'].dt.day
    df = df.drop('Observation date', axis=1)
    return df


def add_location_and_is_naturing_columns(df, location, is_naturing):
    df['Location'] = location
    df['isNaturing'] = is_naturing
    return df


def separate_scientific_name_from_species_column(data, lang):
    if lang == 'kr':
        split = data['Species'].str.split('(', expand=True, n=1)
        split.columns = ['Korean name', 'Species']
        data['Korean name'] = split['Korean name'].str.strip()
        data['Species'] = split['Species'].str[:-1].str.strip()

    else:
        split = data['Species'].str.split('(', expand=True, n=1)
        split.columns = ['English name', 'Species']
        data['English name'] = split['English name'].str.strip()
        data['Species'] = split['Species'].str[:-1].str.strip()

    return data


def get_naturing_raw_data():
    pd.set_option('display.max_columns', None)
    # path = input('네이처링 로우 파일의 위치를 알려주세요.\n')

    # delete later
    path = r'C:\Users\z\Desktop\서울의새\데이터분석\2022-2023\raw_data\06_올림픽공원\naturing\네이처링 올림픽공원_20230514190031.csv'
    naturing_raw_data = pd.read_csv(path, encoding='cp949')
    check_file_is_naturing_raw(naturing_raw_data)

    return naturing_raw_data


def check_file_is_naturing_raw(df):
    if not {'관찰일', '생물이름', '생물분류'}.issubset(df.columns):
        raise ValueError('네이처링 로우 파일 형식이 아닙니다. 확인 후 올바른 파일을 넣어주세요.')
    return


def handle_courses_on_single_day(ebirds_data):
    # 한번에 두코스로 돌았을 경우 최댓값을 남김
    ebirds_data = ebirds_data.sort_values(by='Count', ascending=False)
    ebirds_data = ebirds_data.drop_duplicates(subset=['Korean name', 'Year', 'Month', 'Day'], keep="first")
    ebirds_data = ebirds_data.sort_index()
    return ebirds_data


def handle_record_on_single_month(ebirds_data):
    # 한 달에 두번이상 돌았을 경우 최댓값을 남김
    ebirds_data = ebirds_data.sort_values(by='Count', ascending=False)
    ebirds_data = ebirds_data.drop_duplicates(subset=['Korean name', 'Year', 'Month'], keep="first")
    ebirds_data = ebirds_data.sort_index()
    return ebirds_data


def merge_naturing_to_ebirds(concat_data):
    # 이버드에 기록된 네이처링 기록은 삭제(달 기준)
    return concat_data.drop_duplicates(subset=['Korean name', 'Year', 'Month'], keep="first")

def set_and_return_birds_name_table(data):
    path = r'C:\Users\z\Desktop\birds_name_table.csv'

    existing_name_table = pd.read_csv(path, encoding='ANSI')
    new_name_table = data[['Species', 'Korean name', 'English name']].drop_duplicates(keep='first')
    birds_name_table = pd.concat([existing_name_table, new_name_table])
    birds_name_table = birds_name_table.drop_duplicates(keep='first')

    # birds_name_table.to_csv(r'C:\Users\z\Desktop\birds_name_table.csv', index=False, encoding='ANSI')
    return birds_name_table


def trim_naturing_raw_data(data, location, birds_name_table):
    naturing_data = drop_not_bird_row(data)
    naturing_data = drop_out_dated_row(naturing_data)
    naturing_data = drop_invalid_location_row(naturing_data, location)
    naturing_data = drop_empty_row(naturing_data)
    naturing_data = get_necessary_columns_in_naturing_raw_data(naturing_data)
    naturing_data = trim_date_to_year_month_day(naturing_data)
    naturing_data = insert_required_column(naturing_data, location)
    naturing_data = naturing_data.drop_duplicates()
    naturing_data = add_scientific_name_and_english_name(naturing_data, birds_name_table)

    return naturing_data


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


def drop_empty_row(data):
    data = data.drop(data[data['생물이름'].isnull()].index)
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


def add_scientific_name_and_english_name(data, birds_name_table):
    if not {'Species', 'Korean name', 'English name'}.issubset(birds_name_table.columns):
        raise ValueError('birds_name_table 이 제대로 읽히지 않은 것 같습니다.')

    korean_to_scientific_and_english_name_dict = {}
    for i in range(len(birds_name_table)):
        species = birds_name_table.loc[i, 'Species']
        korean_name = birds_name_table.loc[i, 'Korean name']
        english_name = birds_name_table.loc[i, 'English name']

        if type(species) is str and type(korean_name) is str and type(english_name) is str:
            species = species.strip()
            korean_name = korean_name.strip()
            english_name = english_name.strip()

        korean_to_scientific_and_english_name_dict[korean_name] = [species, english_name]

    data = data.rename(columns={'Species': 'Korean name'})
    data['Species'] = data.apply(
        lambda x: return_species_and_english_name_based_on_korean_name(x['Korean name'], korean_to_scientific_and_english_name_dict)[0], axis=1
    )
    data['English name'] = data.apply(
        lambda x: return_species_and_english_name_based_on_korean_name(x['Korean name'], korean_to_scientific_and_english_name_dict)[1], axis=1
    )

    return data


def return_species_and_english_name_based_on_korean_name(key, table):
    if key in table.keys():
        return table[key]
    else:
        return ['', '']


if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    refine_bss_data()
