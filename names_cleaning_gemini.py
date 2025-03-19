#Это скрипт для очистки списка ФИО с помощью бесплатных версий gemini. Используйте, если вы, как и я, ненавидите регулярные выражения  
import pandas as pd
import time
import os
import google.generativeai as genai  #Перед этим установи !pip install google-generativeai

#Перед тем, как скрамливать модели, советую удалить все знаки препинания и числа, так ей будет легче. У меня датасет состоит из колонки id и Name (фио)

#Настройка API Gemini
genai.configure(api_key="ваш апи из AI Studio")
model = genai.GenerativeModel("gemini-2.0-flash-001")  #Выбор конкретной модели, полный список и ограничения смотри в AI Studio

#Функция очистки 
def clean_names_gemini(df, output_file="cleaned_names.csv"):
    batch_size = 500  #Количество строк в одном запросе. Для gemini 2.0 можно 500, для gemini 1.5 лучше 200
    requests_per_minute = 2  #Ограничение по количеству запросов. Для gemini 2.0 15 запросов в минуту, для gemini 1.5 - два в минуту

    #Если файл существует — удаляем его (чтобы не было дублирования)
    if os.path.exists(output_file):
        os.remove(output_file)

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        #Промт. Советую обязательно давать модели примеры. Мой промт далек от идеала, сделайте круче.......
        prompt = f"""
         Очисти поле 'Name' от всего лишнего, оставив только ФИО на русском. Убери лишние слова, кроме ФИО.
        Удали спецсимволы, цифры, латиницу и лишние пробелы. Если в поле нет ФИО, оставляй исходный вариант. ФИО может быть в любом падеж - изменяй на иминительный. 
        Пример: 'вакансия (временно исполняет Туаев Алан Альбертович)'. Результат: Туаев Алан Альбертович.
        Пример: 'ОМВД России по Вязниковскому району. Для получения информации обратитесь к начальнику пункта Куприну Дмитрию Вячеславовичу тел'. Результат: Куприн Дмитрий Вячеславович.
        Ответь ТОЛЬКО в формате CSV: оставляй исходный столбец id, очищенное ФИО помещай в cleaned_name".

        Данные:
        {batch.to_csv(index=False, header=False)}
        """

        #Отправляем запрос к модели
        response = model.generate_content(prompt)

        if response and hasattr(response, "text") and response.text:
            try:
                csv_text = response.text
                csv_lines = csv_text.splitlines()
                csv_lines = [line for line in csv_lines if line.strip() and not line.startswith("```")] #Фильтрация мусорных строк
                cleaned_csv_text = "\n".join(csv_lines)

                #Парсим очищенный CSV
                cleaned_df = pd.read_csv(StringIO(cleaned_csv_text), on_bad_lines="skip", index_col=False)

                #Сохраняем в один файл 
                cleaned_df.to_csv(output_file, mode='a', index=False, encoding='utf-8', header=not os.path.exists(output_file))

            finally:
                if (i // batch_size + 1) % requests_per_minute == 0:
                    print("Достигнут лимит запросов в минуту, ждем 60 секунд...")
                    time.sleep(60)

    print(f"Данные сохранены в {output_file}")

#Обработка нужного нам датафрейма
df_cleaned = clean_names_gemini(df)

#Далее сверяем с исходным датафреймом, все ли строки обработались. Если нет, вырезаем те, 
#что не обработались, или обработались плохо, прогоняем через этот конвеер снова. Потом заменяем их в исходном датафрейме. 
#Советую делать батчи как можно меньше (в идеале - 200 строк)
