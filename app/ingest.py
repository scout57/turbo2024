import pandas as pd
import traceback
import os
import numpy as np
from pandas.tseries.offsets import DateOffset
import re
import calendar
from datetime import datetime, timedelta

# Имена файлов с входными данными
rsv_prices         = "step1__dataset/Цены РСВ 2022-2023 - зашифрованные.xlsx"
historical_compos  = "step1__dataset/Исторический состав (неполный) - 2023_2024 - зашифрованный.xlsx"
power_station_list = "step1__dataset/Перечень электростанций - зашифрованный.xlsx"
weather            = "step1__dataset/Погода (Нью-Йорк - Бостон).xlsx"
station_indicators = "step1__dataset/Показатели станций 22-23_зашифрованные.xlsx"
price_tut          = "step1__dataset/Цена т.у.т. - зашифрованная.xlsx"

# На прогноз 
composition_forecast = "step1__dataset/Состав на прогноз - зашифрованный.xlsx"
price_tut_forecast   = "step1__dataset/Цена т.у.т. на прогноз - 2024.xlsx"

# Имена указателей на станцию в файлах (п) Столбец1, Column2 итд
col1 = 'Столбец1' # Исторический состав
col2 = 'Column2' # Показатели станций

# Имя директории для выходных данных
output_dir = "step2__ingest"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Имена столбцов
date             = "Дата"
rsv              = "Цена РСВ"

block1           = "1 Блок"
block2           = "2 Блок"
block3           = "3 Блок"
block4           = "4 Блок"
block5           = "5 Блок"
block6           = "6 Блок"
block7           = "7 Блок"
block8           = "8 Блок"
block9           = "9 Блок"
block10          = "10 Блок"

capacity_station = "Установленная мощность станции, МВт"
gen_equip        = "Ген.оборудование"
capacity         = "Установленная мощность, МВт"
capacity_min     = "Минимальная мощность, МВт"

temp_max         = "Максимальная температура"
temp_min         = "Минимальная температура"
temp_avg         = "Средняя температура"
wind_speed       = "Скорость ветра"
precipitation    = "Осадки"
temp_effective   = "Эффективная температура"

gen_energy       = "Выработка тыс.кВтч"
own_use_pct      = "Собственные нужды э/э на выработку э/э %"
own_use_gen      = "Собственные нужды э/э на выработку э/э тыс.кВтч"
fuel_cons_el     = "Удельный расход топлива на э/э г/кВтч"
fuel_cons_rel    = "Расход топлива на отпуск э/э т.у.т."
heat_rel         = "Отпуск тепла Гкал"
own_use_heat1    = "Собственные нужды э/э на отпуск тепла кВтч/ГКал"
own_use_heat2    = "Собственные нужды э/э на отпуск тепла тыс.кВтч"
fuel_cons_ht     = "Удельный расход топлива на тепло кг/ГКал"
fuel_cons_ht_rel = "Расход топлива на отпуск тепла т.у.т."
rel_from_bus     = "Отпуск э/э с шин тыс.кВтч"
total_fuel_cons  = "Общий расход условного топлива т.у.т."

tut              = "Цена т.у.т."
multi_tut        = "Стоимость т.у.т."

# Словарь вида Город:Станция
city_gtp_dict = {}

def normalize_column_name(name):
    """
    Функция для нормализации названий столбцов:
    - Приводит к нижнему регистру
    - Удаляет диакритические знаки
    """
    if isinstance(name, str):
        # Заменяем "ё" на "е", убираем пробелы, спецсимволы и приводим к нижнему регистру
        normalized = name.replace("ё", "е").replace("Ё", "Е")
        return re.sub(r'[^\w\s]', '', normalized).replace(" ", "").lower()
    return name  # На случай, если name не строка

def create_rsv_dataframe():
    """
    Извлекает данные из файла "Цены РСВ" и создаёт датафрейм по нему.
    """
    try:
        df = pd.read_excel(rsv_prices)
        # Преобразование столбца 'Дата' в формат datetime с округлением до часа
        df[date] = pd.to_datetime(df[date], format='%d.%m.%Y %H:%M').dt.floor('h')
        return df
    
    except Exception as e:
        print("Ошибка при создании начального DataFrame - Цены РСВ:", e)
        traceback.print_exc()
        raise e
    
def create_historical_compos_dataframe():
    """
    Извлекает данные из файла "Исторический состав", собирает дату из столбцов\n
    "Месяц", "Число", "Час" и имени листа (год), и добавляет их в датафрейм
    """
    try:
        xls = pd.ExcelFile(historical_compos)
        
        dataframes = []
        # Обрабатываем только листы с '_ч' в конце имени
        for sheet_name in xls.sheet_names:
            if sheet_name.endswith('_ч'):
                # Извлечение года из имени листа
                year = int(sheet_name.split('_')[0])
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Создаем новый столбец с полной датой
                df[date] = pd.to_datetime({
                    'year': [year] * len(df),
                    'month': df['МЕСЯЦ'],
                    'day': df['Число'],
                    'hour': df['Час']
                }).dt.floor('h')

                df_filtered = df[[date, col1, '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']]
                dataframes.append(df_filtered)

        full_df = pd.concat(dataframes, ignore_index=True)
        return full_df
    
    except Exception as e:
        print("Ошибка при создании начального DataFrame - Исторический состав:", e)
        raise e
    
def create_station_indicators_dataframe():
    """
    Извлекает данные из файла "Показатели станций", собирает дату из столбцов,
    добавляет её в датафрейм вместе с остальными атрибутами/значениями.\n
    Производит пивотирование данных с индексами Дата и Column2.
    """
    try:
        df = pd.read_excel(station_indicators)
        # Повторение каждой строки 24 раза (для каждого часа)
        df_expanded = df.loc[df.index.repeat(24)].reset_index(drop=True)
        # Генерация столбца с часами от 0 до 23
        df_expanded['Час'] = np.tile(range(24), len(df_expanded) // 24 + 1)[:len(df_expanded)]
        # Создаем новый столбец с полной датой
        df_expanded[date] = pd.to_datetime({
            'year': df_expanded['Год'],
            'month': df_expanded['Месяц'],
            'day': df_expanded['День'],
            'hour': df_expanded['Час']
        }).dt.floor('h')

        # Пивотирование данных
        pivot_df = df_expanded.pivot_table(
        index=[date, col2],  # Индексы
        columns='Атрибут',   # Столбец для создания столбцов в новом DataFrame
        values='Значение',   # Значения, которые будут заполнены
        aggfunc='sum'        # Функция агрегирования
        ).reset_index()  # Сброс индекса для уникализации
        
        # Убираем проблемы из всех столбцов со значениями
        columns_to_process = [gen_energy, own_use_pct, own_use_gen, fuel_cons_el, 
                    fuel_cons_rel, heat_rel, own_use_heat1, own_use_heat2,  
                    fuel_cons_ht, fuel_cons_ht_rel, rel_from_bus, total_fuel_cons]
        for col in columns_to_process:
            # Преобразование в числа и деление на 24 для распределения значений по часам
            pivot_df[col] = pivot_df[col].astype(str).str.replace(' ', '').str.replace('\xa0', '').str.replace(',', '.').astype(float) / 24
        return pivot_df[[date, col2] + columns_to_process]

    except Exception as e:
        print(f"Ошибка при создании начального DataFrame - Показатели станций: {e}")
        raise e
    
def create_tut_dataframe():
    """
    Извлекает данные из файла "Цена т.у.т.", собирает дату из столбцов,
    добавляет к ней распределение по дням и часам и записывает её в датафрейм.
    """
    try:
        df = pd.read_excel(price_tut)
        # Преобразуем данные 'Год' и 'Месяц' в строки
        df['Год-Месяц'] = pd.to_datetime(df[['Год', 'Месяц']].astype(str).agg('-'.join, axis=1))
        
        # Создаем полный диапазон часов для каждого месяца
        full_data = []
        for _, row in df.iterrows():
            # Начало и конец месяца
            start_date = row['Год-Месяц']
            # Получаем количество дней в месяце
            month_days = calendar.monthrange(start_date.year, start_date.month)[1]
            # Создаем временной диапазон для каждого дня
            for day in range(1, month_days + 1):
                day_date = start_date.replace(day=day)
                hourly_range = pd.date_range(start=day_date, end=day_date + pd.Timedelta(days=0, hours=23), freq='h')
                for time in hourly_range:
                    new_row = row.copy()
                    new_row[date] = time.floor('h')
                    full_data.append(new_row)

        df_hourly = pd.DataFrame(full_data)
        return df_hourly.drop(columns=['Год-Месяц', 'Год', 'Месяц'])

    except Exception as e:
        print(f"Ошибка при создании начального DataFrame - Цена т.у.т.: {e}")
        raise e

def get_rsv_by_name(df, station_name):
    """
    Возвращает новый DataFrame Цены РСВ по имени станции\n
    из DataFrame в формате 'Дата' и 'Цена РСВ'.
    
    Аргументы
        df (pd.DataFrame): DataFrame, содержащий данные.
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        normalized_station_name = normalize_column_name(station_name)
        normalized_columns = [normalize_column_name(col) for col in df.columns]
        if normalized_station_name in normalized_columns:
            # Определяем оригинальное имя столбца
            original_station_name = df.columns[normalized_columns.index(normalized_station_name)]
            # Проверяем, что оба столбца ('Дата' и station_name) существуют и корректны
            if date not in df.columns:
                raise ValueError("Столбец 'Дата' не найден в DataFrame.")
            
            # Создаём новый DataFrame с извлечёнными данными
            return pd.DataFrame({
                date: df[date],
                rsv: df[original_station_name]
            })
        else:
            print(f"Станция '{station_name}' не найдена в DataFrame.")
            return None
    
    except Exception as e:
        print(f"Ошибка при извлечении '{station_name}' - Цена РСВ: {e}")
        return None

def get_hs_by_name(df, station_name):
    """
    Возвращает новый DataFrame Исторический состав по имени станции\n
    из DataFrame в формате Дата и блоки (1-10).
    
    Аргументы
        df (pd.DataFrame): DataFrame, содержащий данные.
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        filtered_df = df[df[col1].apply(normalize_column_name) == normalize_column_name(station_name)]

        if not filtered_df.empty:
            return pd.DataFrame({
                    date: filtered_df[date],
                    block1: filtered_df['1'],
                    block2: filtered_df['2'],
                    block3: filtered_df['3'],
                    block4: filtered_df['4'],
                    block5: filtered_df['5'],
                    block6: filtered_df['6'],
                    block7: filtered_df['7'],
                    block8: filtered_df['8'],
                    block9: filtered_df['9'],
                    block10: filtered_df['10'],
                }).reset_index(drop=True)
        else:
            print(f"Станция '{station_name}' не найдена в DataFrame.")
            return None
    
    except Exception as e:
        print(f"Ошибка при извлечении '{station_name}' - Исторический состав: {e}")
        return None

def get_si_by_name(df, station_name):
    """
    Возвращает новый DataFrame Показатели станций с данными по имени\n
    станции, в формате Дата, Атрибуты - Значения
    
    Аргументы:
        df (pd.DataFrame): Полный DataFrame с данными.
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        filtered_df = df[df[col2].apply(normalize_column_name) == normalize_column_name(station_name)]

        if not filtered_df.empty:
            # Возврат датафрейма без столбца с именем станции (col2)
            return pd.DataFrame({
                date: filtered_df[date],
                gen_energy: filtered_df[gen_energy],
                own_use_pct: filtered_df[own_use_pct],
                own_use_gen: filtered_df[own_use_gen],
                fuel_cons_el: filtered_df[fuel_cons_el],
                fuel_cons_rel: filtered_df[fuel_cons_rel],
                heat_rel: filtered_df[heat_rel],
                own_use_heat1: filtered_df[own_use_heat1],
                own_use_heat2: filtered_df[own_use_heat2],
                fuel_cons_ht: filtered_df[fuel_cons_ht],
                fuel_cons_ht_rel: filtered_df[fuel_cons_ht_rel],
                rel_from_bus: filtered_df[rel_from_bus],
                total_fuel_cons: filtered_df[total_fuel_cons],
            }).reset_index(drop=True)
        else:
            print(f"Станция '{station_name}' не найдена в DataFrame.")
            return None

    except Exception as e:
        print(f"Ошибка при извлечении '{station_name}' - Показатели станций: {e}")
        return None
    
def get_tut_by_name(df, station_name):
    """
    Возвращает срез начального DataFrame Цена т.у.т. с данными по имени\n
    станции, в формате Дата и Цена т.у.т., заранее переименовав столбец\n
    по имени станции на "Цена т.у.т.".
    
    Аргументы:
        df (pd.DataFrame): Полный DataFrame с данными.
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        normalized_station_name = normalize_column_name(station_name)
        normalized_columns = [normalize_column_name(col) for col in df.columns]
        if normalized_station_name in normalized_columns:
            original_station_name = df.columns[normalized_columns.index(normalized_station_name)]
            if date not in df.columns:
                raise ValueError("Столбец 'Дата' не найден в DataFrame.")
            
            # Возвращаем срез DataFrame и переименовываем столбец station_name
            return df[[date, original_station_name]].dropna().rename(columns={original_station_name: tut})
        else:
            print(f"Станция '{station_name}' не найдена в DataFrame.")
            return None
    
    except Exception as e:
        print(f"Ошибка при извлечении '{station_name}' - Цена т.у.т: {e}")
        return None
    
def get_capacity_by_name(station_name):
    """
    Возвращает данные по установленной и минимальной мощности блоков,\n
    а также мощность всей станции в МВт.

    Аргументы:
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        global city_gtp_dict
        xls = pd.ExcelFile(power_station_list)
        sheet_names = xls.sheet_names
        dfs = []

        for sheet_name in sheet_names:
            try:
                year = int(sheet_name)
                df = pd.read_excel(xls, sheet_name=sheet_name)
                df['Год'] = year
                dfs.append(df)
            except ValueError:
                print(f"Пропущен лист '{sheet_name}', так как его название не является годом.")
                continue
        
        data = pd.concat(dfs, ignore_index=True)
        data_station = data[data['Наименование ГТП генерации'] == station_name].copy()
        
        if data_station.empty:
            raise ValueError(f"Станция с названием '{station_name}' не найдена в данных.")
        
        city_gtp_dict = data[data['Наименование ГТП генерации'] == station_name][['Город', 'Наименование ГТП генерации']].drop_duplicates().set_index('Город')['Наименование ГТП генерации'].to_dict()

        hourly_data = []
        for year in data['Год'].unique():
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23)
            date_range = pd.date_range(start=start_date, end=end_date, freq='h')
            year_data = data_station[data_station['Год'] == year]
            station_power = year_data['установленная мощность станции, МВт'].iloc[0] / 24

            block_data = []
            for idx, row in year_data.iterrows():
                block_info = {
                    f"Установленная мощность блока {row['Ген.оборудование']}, МВт": row['установленная мощность, МВт'] / 24,
                    f"Минимум блока {row['Ген.оборудование']}, МВт": row['минимум'] / 24
                }
                block_data.append(block_info)
            
            hourly_df = pd.DataFrame(index=date_range)
            hourly_df[date] = date_range
            hourly_df[capacity_station] = station_power
            
            for block_info in block_data:
                for col, value in block_info.items():
                    hourly_df[col] = value
            hourly_data.append(hourly_df)
        
        result_df = pd.concat(hourly_data).reset_index(drop=True)
        result_df.fillna(0, inplace=True)
        return result_df
    
    except Exception as e:
        print(f"Ошибка при обработке данных станции '{station_name}': {e}")
        return None

def get_weather_by_name(station_name):
    """
    Возвращает данные по погоде для города, соответствующего указанной станции.
    
    Аргументы:
        station_name (str): Имя станции, данные которой нужно извлечь.
    """
    try:
        city_name = next((city for city, station in city_gtp_dict.items() if station == station_name), None)
        if not city_name:
            print(f"Город для станции '{station_name}' не найден в словаре.")
            return pd.DataFrame()

        xls_weather = pd.ExcelFile(weather)
        if city_name not in xls_weather.sheet_names:
            print(f"Погодные данные для города '{city_name}' отсутствуют.")
            return pd.DataFrame()

        weather_data = pd.read_excel(xls_weather, sheet_name=city_name)
        weather_data.columns = weather_data.columns.map(normalize_column_name)
        hourly_weather_data = []
        for _, row in weather_data.iterrows():
            day_date = pd.to_datetime(row["дата"])
            for hour in range(24):
                hourly_weather_data.append({
                    date: day_date + timedelta(hours=hour),
                    temp_max: row["максимальнаятемпература"],
                    temp_min: row["минимальнаятемпература"],
                    temp_avg: row["средняятемпература"],
                    wind_speed: row["скоростьветра"],
                    #precipitation: row["осадки"],
                    temp_effective: row["эффективнаятемпература"]
                })

        hourly_weather_df = pd.DataFrame(hourly_weather_data)
        return hourly_weather_df

    except Exception as e:
        print(f"Ошибка при получении погодных данных для станции '{station_name}': {e}")
        return None

def main():
    """Пример, как мы разбираем данные по датафрейму и имени станции"""
    try:
        rsv_df = create_rsv_dataframe()  # Собрали DataFrame РСВ
        hc_df = create_historical_compos_dataframe()  # Собрали DataFrame Ист. Состав
        si_df = create_station_indicators_dataframe()  # Собрали DataFrame Показатели станций
        tut_df = create_tut_dataframe() # Собрали DataFrame Цена т.у.т

        # Перечень столбцов, которые нужно извлечь
        stations_to_extract = rsv_df.columns[rsv_df.columns != date]  # Все столбцы, кроме 'Дата'

        for station in stations_to_extract:
            dataframes = [
                get_rsv_by_name(rsv_df, station),
                get_hs_by_name(hc_df, station),
                get_si_by_name(si_df, station),
                get_tut_by_name(tut_df, station),
                get_capacity_by_name(station),
                get_weather_by_name(station)
            ]
            # Фильтруем только те датафреймы, которые не являются None и не пустые
            dataframes = [df for df in dataframes if df is not None and not df.empty]

            if dataframes:
                try:
                    for df in dataframes:
                        df.set_index(date, inplace=True)

                    merged_df = pd.concat(dataframes, axis=1, join='outer')
                    merged_df.reset_index(inplace=True)
                    merged_df.sort_values(by=date, inplace=True)
                    merged_df.dropna(axis=1, how='all', inplace=True)
                    merged_df.dropna(inplace=True)
                    merged_df[multi_tut] = merged_df[total_fuel_cons] * merged_df[tut]

                    file_path = os.path.join(output_dir, f"{station}.xlsx")
                    merged_df.to_excel(file_path, index=False)
                    print(f"Данные для '{station}' сохранены как: {file_path}")

                except Exception as e:
                    print(f"Ошибка при записи в файл информации по '{station}': {e}")

    except Exception as e:
        print(f"Общая ошибка: {e}")

if __name__ == "__main__":
    main()
