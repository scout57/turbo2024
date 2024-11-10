import pandas as pd
from prophet import Prophet
import glob
import os
import re
import matplotlib.pyplot as plt

output_dir = "step3__forecast"

# Функция для очистки имени файла
def sanitize_filename(filename):
    # Заменяем все недопустимые символы на знак подчеркивания
    return re.sub(r'[\\/:"*?<>|]+', '_', filename)

# Функция для прогнозирования и сохранения графиков
def forecast_station(data_daily, station_name, target_column, plots_dir):
    # Фильтруем данные по станции
    df_station = data_daily[data_daily['СТАНЦИЯ'] == station_name]
    
    # Подготавливаем данные для Prophet
    df_prophet = df_station[['Дата', target_column]].rename(columns={'Дата': 'ds', target_column: 'y'})
    
    # Проверка на пропуски
    if df_prophet['y'].isnull().any():
        print(f"Предупреждение: Есть пропуски в данных для станции {station_name}, показателя {target_column}. Заполняем пропуски методом прямого заполнения.")
        df_prophet['y'].fillna(method='ffill', inplace=True)
        df_prophet['y'].fillna(method='bfill', inplace=True)
    
    # Инициализируем модель Prophet с настройками
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode='additive'
    )
    model.fit(df_prophet)
    
    # Создаем будущие даты для прогнозирования с 01.01.2024 по 30.06.2024
    future_dates = pd.date_range(start='2024-01-01', end='2024-06-30')
    future = pd.DataFrame({'ds': future_dates})
    
    # Прогнозируем
    forecast = model.predict(future)
    
    # Добавляем название станции
    forecast['СТАНЦИЯ'] = station_name
    
    # Оставляем только нужные столбцы
    forecast_needed = forecast[['СТАНЦИЯ', 'ds', 'yhat']]
    
    # Замена отрицательных значений на 0
    forecast_needed['yhat'] = forecast_needed['yhat'].clip(lower=0)
    
    # Построение и сохранение графика
    plt.figure(figsize=(10, 6))
    model.plot(forecast)
    plt.title(f'Прогноз для станции {station_name} - {target_column}')
    
    # Создаем имя файла для графика
    plot_filename = f'прогноз_{sanitize_filename(station_name)}_{sanitize_filename(target_column)}.png'
    plot_path = os.path.join(plots_dir, plot_filename)
    
    # Сохраняем график
    plt.savefig(plot_path)
    plt.close()
    
    print(f"График сохранен: {plot_path}")
    
    return forecast_needed

# Чтение файлов Excel
path = 'step2__ingest/'
excel_files = glob.glob(os.path.join(path, '*.xlsx'))

data = pd.DataFrame()

for file in excel_files:
    # Извлекаем название станции из имени файла
    station_name = os.path.splitext(os.path.basename(file))[0]
    
    # Читаем данные из файла Excel
    df = pd.read_excel(file)
    
    # Добавляем столбец с названием станции
    df['СТАНЦИЯ'] = station_name
    
    # Преобразуем 'Дата' в datetime
    df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')  # Добавляем обработку ошибок
    
    # Удаляем строки с некорректными датами
    df = df.dropna(subset=['Дата'])
    
    # Объединяем данные
    data = pd.concat([data, df], ignore_index=True)

# Агрегируем данные до суточных суммарных значений
data_daily = data.groupby(['СТАНЦИЯ', pd.Grouper(key='Дата', freq='D')]).sum().reset_index()

# Опционально: Замена '/' на '_' в именах столбцов для удобства
data_daily.columns = data_daily.columns.str.replace('/', '_')

# Проверка имен столбцов
print("Имена столбцов в data_daily:")
print(data_daily.columns.tolist())

# Словарь для хранения прогнозов по каждому показателю
forecasts = {
    'Общий расход условного топлива т.у.т.': [],
    'Расход топлива на отпуск э_э т.у.т.': [],  # Используем 'э_э' после замены
    'Расход топлива на отпуск тепла т.у.т.': []
}

# Убедитесь, что все ключи присутствуют в data_daily
for key in forecasts.keys():
    if key not in data_daily.columns:
        print(f"Ошибка: Столбец '{key}' отсутствует в data_daily.")
        print("Проверьте имена столбцов и обновите словарь 'forecasts' соответственно.")
        exit(1)

stations = data_daily['СТАНЦИЯ'].unique()

# Создаем директорию для сохранения графиков
plots_dir = os.path.join(output_dir, 'plots')
os.makedirs(plots_dir, exist_ok=True)

for station in stations:
    for target in forecasts.keys():
        print(f"Прогнозирование для станции '{station}', показателя '{target}'...")
        forecast_df = forecast_station(data_daily, station, target, plots_dir)
        forecasts[target].append(forecast_df)

# Сохранение результатов в CSV файлы
for target, forecast_list in forecasts.items():
    # Объединяем прогнозы по всем станциям для данного показателя
    result_df = pd.concat(forecast_list, ignore_index=True)
    
    # Переименовываем столбцы согласно требуемой структуре
    result_df = result_df.rename(columns={'ds': 'дата', 'yhat': 'Значение'})
    result_df = result_df[['СТАНЦИЯ', 'дата', 'Значение']]
    
    # Очищаем имя файла
    filename = os.path.join(output_dir, f'прогноз_{sanitize_filename(target)}.csv')
    result_df.to_csv(filename, index=False, sep=',', encoding='utf-8-sig')
    
    print(f"CSV файл сохранен: {filename}")

print("Прогнозирование и построение графиков завершено успешно. CSV файлы и графики сохранены.")
