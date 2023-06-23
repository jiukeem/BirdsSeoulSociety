import pandas as pd
import glob
import os
import re
import Config


def refine_bss_data():
    # 1. get location
    location = get_location()

    # 2. refine two raw reference_data - ebirds & naturing
    ebirds_basic, ebirds_merged, birds_name_table = process_ebirds_data(location)
    naturing = process_naturing_data(location, birds_name_table)

    # 3. concat two refined reference_data
    basic_data = pd.concat([ebirds_basic, naturing])

    # +alpha: just changing columns order
    basic_data = basic_data[
        ['Cornell index', 'Species', 'English name', 'Korean name', 'Count', 'Year', 'Month', 'Day', 'Location', 'isNaturing']]

    # TODO refactor
    merged_data = pd.concat([ebirds_merged, naturing])
    merged_data = merged_data[
        ['Cornell index', 'Species', 'English name', 'Korean name', 'Count', 'Year', 'Month', 'Day', 'Location', 'isNaturing']]
    merged_data = drop_duplicate_btw_ebird_and_naturing(merged_data)

    # num of observed species per month
    species_num_per_month = merged_data.groupby(['Year', 'Month']).size().reset_index(name='Species count')
    species_num_per_month['Location'] = location
    species_num_per_month.columns = ['Year', 'Month', 'Species count', 'Location']

    output_file = f'./output/0{Config.location_dict[location]}_{location}_22-23.xlsx'
    writer = pd.ExcelWriter(output_file)

    # Write each dataframe to a different sheet in the Excel file
    basic_data.to_excel(writer, sheet_name='basic', index=False)
    merged_data.to_excel(writer, sheet_name='merged', index=False)
    species_num_per_month.to_excel(writer, sheet_name='num of species per month', index=False)

    writer._save()
    return


def get_location():
    location = input('다음 중 탐조 장소를 선택해주세요.\n여의샛강, 어린이대공원, 북서울꿈의숲, 창경궁, 남산, 올림픽공원, 푸른수목원, 강서습지생태공원, 중랑천\n')

    while location not in Config.location_dict.keys():
        print('장소가 올바르게 인식되지 않았습니다. 띄어쓰기 없이 다 붙여 입력해주세요.\n')
        location = input()

    return location


def process_ebirds_data(location):
    # 1. get each raw reference_data list
    ebirds_raw_data_list_kr = get_ebirds_raw_data_list(lang='kr')
    ebirds_raw_data_list_en = get_ebirds_raw_data_list(lang='en')

    # 2. trim reference_data list and concat to one reference_data
    ebirds_trimmed_concat_data_kr = trim_ebirds_raw_data(ebirds_raw_data_list_kr, location, lang='kr')
    ebirds_trimmed_concat_data_en = trim_ebirds_raw_data(ebirds_raw_data_list_en, location, lang='en')

    # 3. merge kr & en reference_data
    ebirds_final_data = merge_ebirds_kr_with_en_data(kr_data=ebirds_trimmed_concat_data_kr,
                                                     en_data=ebirds_trimmed_concat_data_en)

    # 4. handle exception case & update bird name table from result reference_data
    ebirds_final_data = process_exception_names(ebirds_final_data)
    birds_name_table = update_and_return_birds_name_table(ebirds_final_data)

    # note. 아래 코드는 불필요해 보일 수 있는데
    # 영문명 끝에 (eurasian 이라고 표기된 것을 birds_name_table 을 이용해 (eurasian) 으로 고치거나
    # dusky x nomann's thrush 같은 애들을 바로잡기 위함. 위의 process_exception_names 에서 한글명을 바로잡고 그에 상응하는
    # Species 와 English name 으로 변경해주는 역할.
    ebirds_final_data = add_scientific_name_and_english_name(ebirds_final_data, birds_name_table, is_naturing=False)

    # save max count in two courses records on a single day
    ebirds_merged_data = handle_courses_on_single_day(ebirds_final_data)
    # save max count in two records on a single month
    ebirds_merged_data = handle_record_on_single_month(ebirds_merged_data)
    return ebirds_final_data, ebirds_merged_data, birds_name_table


def process_naturing_data(location, birds_name_table):
    naturing_raw_data = get_naturing_raw_data()
    naturing_trimmed_data = trim_naturing_raw_data(naturing_raw_data, location, birds_name_table)

    # 집비둘기는 세지 않음~
    naturing_trimmed_data.drop(naturing_trimmed_data[naturing_trimmed_data['Korean name'].astype('string') == '집비둘기'].index, inplace=True)
    return naturing_trimmed_data


def trim_ebirds_raw_data(data_list, location, lang='kr'):
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    # for debugging

    temp_ebirds_trimmed_data_list = []

    for data in data_list:
        data = get_necessary_columns_in_ebird_raw_data(data)
        separate_scientific_name_from_species_column(data, lang)
        data = split_observation_date(data)
        data = add_location_and_is_naturing_columns(data, location, is_naturing=False)
        temp_ebirds_trimmed_data_list.append(data)

    ebirds_trimmed_concat_data = pd.concat(temp_ebirds_trimmed_data_list, axis=0, ignore_index=True)
    return ebirds_trimmed_concat_data


def get_ebirds_raw_data_list(lang='kr'):
    ebirds_raw_data_list = []

    if lang == 'kr':
        path = input('ebirds kr raw 파일들이 들어있는 폴더 경로를 알려주세요\n')
    elif lang == 'en':
        path = input('ebirds en raw 파일들이 들어있는 폴더 경로를 알려주세요\n')
    else:
        raise ValueError('코드 오류: get_ebirds_raw_data_list param error')

    files_path = glob.glob(os.path.join(path, "*.csv"))
    for file_path in files_path:
        df = pd.read_csv(file_path)
        check_file_is_ebird_raw(df)
        ebirds_raw_data_list.append(df)

    if len(ebirds_raw_data_list) == 0:
        raise ValueError('이버드 raw 파일을 가져오지 못했습니다. 올바른 경로인지 확인하고 다시 입력해주세요.\n')


    return ebirds_raw_data_list


def check_file_is_ebird_raw(df):
    if not {'Observation date', 'Species', 'Count'}.issubset(df.columns):
        raise ValueError('이버드 로우 파일 형식이 아닙니다. 확인 후 올바른 파일을 넣어주세요.' + df.head())
    return


def merge_ebirds_kr_with_en_data(kr_data, en_data):
    merged_df = pd.merge(kr_data, en_data[['English name']], left_index=True, right_index=True)
    return merged_df


def get_necessary_columns_in_ebird_raw_data(df):
    return df[['Species', 'Count', 'Observation date']]


def separate_scientific_name_from_species_column(data, lang):
    split = data[['Species']].copy()

    def split_value(value):
        if value.count('(') > 1:
            first_open = value.find('(')
            second_open = value.find('(', first_open + 1)
            first_close = value.find(')')
            if first_close > second_open:
                return value.split('(', 1)
            else:
                return value.split(')', 1)
        else:
            return value.split('(', 1)

    def strip_name_value(value):
        pattern = r'^\(|\)$'
        stripped_value = re.sub(pattern, '', value).strip()
        return stripped_value

    def strip_species_value(value):
        pattern = r'\((.*?)\)|\((.*)$|[\(\)]'
        stripped_value = re.sub(pattern, lambda x: x.group(1) or x.group(2) or '', value).strip()
        return stripped_value

    if lang == 'kr':
        split['Species'] = split['Species'].apply(split_value)
        split = pd.DataFrame(split['Species'].tolist(), columns=['Korean name', 'Species'])
        data['Korean name'] = split['Korean name'].apply(strip_name_value)
        data['Species'] = split['Species'].apply(strip_species_value)

    else:
        split['Species'] = split['Species'].apply(split_value)
        split = pd.DataFrame(split['Species'].tolist(), columns=['English name', 'Species'])
        data['English name'] = split['English name'].apply(strip_name_value)
        data['Species'] = split['Species'].apply(strip_species_value)

    return data


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

    if is_naturing is True:
        df['Count'] = 0

    return df


def get_naturing_raw_data():
    pd.set_option('display.max_columns', None)
    path = input('네이처링 로우 파일의 위치를 알려주세요.\n')

    naturing_raw_data = pd.read_csv(path, encoding='cp949')
    check_file_is_naturing_raw(naturing_raw_data)

    return naturing_raw_data


def check_file_is_naturing_raw(df):
    if not {'관찰일', '생물이름', '생물분류'}.issubset(df.columns):
        raise ValueError('네이처링 로우 파일 형식이 아닙니다. 확인 후 올바른 파일을 넣어주세요.')
    return


def handle_courses_on_single_day(ebirds_data):
    ebirds_data = ebirds_data.sort_values(by='Count', ascending=False)
    ebirds_data = ebirds_data.drop_duplicates(subset=['Korean name', 'Year', 'Month', 'Day'], keep="first")
    ebirds_data = ebirds_data.sort_index()
    return ebirds_data


def handle_record_on_single_month(ebirds_data):
    ebirds_data = ebirds_data.sort_values(by='Count', ascending=False)
    ebirds_data = ebirds_data.drop_duplicates(subset=['Korean name', 'Year', 'Month'], keep="first")
    ebirds_data = ebirds_data.sort_index()
    return ebirds_data


def drop_duplicate_btw_ebird_and_naturing(concat_data):
    # 이버드에 기록된 네이처링 기록은 삭제(달 기준)
    return concat_data.drop_duplicates(subset=['Korean name', 'Year', 'Month'], keep="first")


def update_and_return_birds_name_table(data):
    path = './reference_data/birds_name_table.csv'

    existing_name_table = pd.read_csv(path, encoding='ANSI')
    new_name_table = data[['Species', 'Korean name', 'English name']].drop_duplicates(keep='first')
    birds_name_table = pd.concat([existing_name_table, new_name_table])
    birds_name_table = birds_name_table.drop_duplicates(subset='Korean name', keep='first')

    birds_name_table = add_cornell_index(birds_name_table)

    birds_name_table.to_csv(path, index=False, encoding='ANSI')
    return birds_name_table


def process_exception_names(data):
    data.loc[data['Korean name'] == '촉새 / 섬촉새', 'Korean name'] = '촉새'
    data.loc[data['Korean name'] == '재갈매기 / 한국재갈매기', 'Korean name'] = '재갈매기'
    data.loc[data['Korean name'] == 'Dusky x Naumann\'s Thrush (hybrid', 'Korean name'] = '개똥지빠귀 x 노랑지빠귀 (교잡종)'
    return data


def trim_naturing_raw_data(data, location, birds_name_table):
    naturing_data = drop_non_bird_row(data)
    naturing_data = drop_out_dated_row(naturing_data)
    naturing_data = fill_location_empty_row(naturing_data, location)
    naturing_data = drop_invalid_location_row(naturing_data, location)
    naturing_data = drop_empty_row(naturing_data)
    naturing_data = get_necessary_columns_in_naturing_raw_data(naturing_data)
    naturing_data = trim_date_to_year_month_day(naturing_data)
    naturing_data = add_location_and_is_naturing_columns(naturing_data, location, is_naturing=True)
    naturing_data = naturing_data.drop_duplicates()
    naturing_data = add_scientific_name_and_english_name(naturing_data, birds_name_table)

    return naturing_data


def drop_non_bird_row(data):
    data = data[data['생물분류'] == '조류']
    data = data.drop('생물분류', axis=1)
    return data


def drop_out_dated_row(data):
    year = input('기록년도를 입력해주세요. 2022년 4월부터 2023년 3월까지일 경우 2022로 입력합니다.\n')
    is_valid = False

    while not is_valid:
        try:
            number = int(year)
            if number < 2018 or number > 2024:
                year = input('올바른 입력값이 아닙니다. 2018년도부터 2022년도 중 해당하는 년도를 숫자만 입력해주세요\n')
            else:
                is_valid = True
        except ValueError:
            year = input('숫자만 입력해주세요\n')

    data['관찰일'] = pd.to_datetime(data['관찰일'])
    return data[(data['관찰일'] > f'{int(year)}-04-01') & (data['관찰일'] < f'{int(year)+1}-03-01')]


def fill_location_empty_row(df, location):
    average_longitude = sum(Config.location_longitude_dict[location])/2
    average_latitude = sum(Config.location_latitude_dict[location])/2

    df['경도'] = df['경도'].fillna(average_longitude)
    df['위도'] = df['위도'].fillna(average_latitude)
    return df


def drop_invalid_location_row(data, location):
    data = drop_invalid_location_row_with_longitude(data, location)
    data = drop_invalid_location_row_with_latitude(data, location)

    return data


def drop_invalid_location_row_with_longitude(data, location):
    data['경도'] = pd.to_numeric(data['경도'])
    min_longitude = Config.location_longitude_dict[location][0]
    max_longitude = Config.location_longitude_dict[location][1]
    data = data[(min_longitude < data['경도']) & (data['경도'] < max_longitude)]

    return data


def drop_invalid_location_row_with_latitude(data, location):
    data['위도'] = pd.to_numeric(data['위도'])
    min_latitude = Config.location_latitude_dict[location][0]
    max_latitude = Config.location_latitude_dict[location][1]
    data= data[(min_latitude < data['위도']) & (data['위도'] < max_latitude)]

    return data


def drop_empty_row(data):
    data = data.drop(data[data['생물이름'].isnull()].index)
    return data


def get_necessary_columns_in_naturing_raw_data(data):
    data = data[['관찰일', '생물이름']]
    return data.rename(columns={'생물이름': 'Korean name'})


def trim_date_to_year_month_day(data):
    data['Year'] = data['관찰일'].dt.year
    data['Month'] = data['관찰일'].dt.month
    data['Day'] = data['관찰일'].dt.day
    data = data.drop('관찰일', axis=1)

    return data


def add_scientific_name_and_english_name(data, birds_name_table, is_naturing=True):
    if not {'Species', 'Korean name', 'English name'}.issubset(birds_name_table.columns):
        raise ValueError('birds_name_table 이 제대로 읽히지 않은 것 같습니다.')

    if not is_naturing:
        data = data.drop(columns=['Species', 'English name'], axis=1)

    data = pd.merge(data, birds_name_table, on='Korean name', how='left')
    return data


def add_cornell_index(birds_name_table):
    cornell_path = './reference_data/Clements-v2022-October-2022.xlsx'

    # generate name to idx dictionary from ebird
    cornell_df = pd.read_excel(cornell_path)[['scientific name', 'sort v2021']]

    name_to_idx = {}
    for i in range(len(cornell_df)):
        name = cornell_df.loc[i, "scientific name"]
        idx = cornell_df.loc[i, "sort v2021"]

        if name in name_to_idx:
            print(f"duplicated species: {name}")
        else:
            name_to_idx[name] = idx

    def get_index(row):
        species = row['Species']
        if species not in name_to_idx:
            print(f'NOTICE: {species} not in mapping table')
            return 0

        return name_to_idx[species]

    birds_name_table['Cornell index'] = birds_name_table.apply(lambda x: get_index(x), axis=1)
    birds_name_table['Cornell index'] = birds_name_table['Cornell index'].astype('int')

    return birds_name_table


if __name__ == '__main__':
    # ignore pandas warning
    pd.options.mode.chained_assignment = None

    refine_bss_data()