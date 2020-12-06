import json
import os
import requests
import time

# Получаем перечень ранее созданных файлов со списком вакансий и проходимся по нему в цикле
for fl in os.listdir(r'C:\Users\Asus\Documents\GitHub\OTUS-2020-2021\js_files'):

    # Открываем файл, читаем его содержимое, закрываем файл
    f = open(r'C:\Users\Asus\Documents\GitHub\OTUS-2020-2021\\js_files\{}'.format(fl), encoding='utf8')
    jsonText = f.read()
    f.close()

    # Преобразуем полученный текст в объект справочника
    jsonObj = json.loads(jsonText)

    # Получаем и проходимся по непосредственно списку вакансий
    for v in jsonObj['items']:

        # Обращаемся к API и получаем детальную информацию по конкретной вакансии
        req = requests.get(v['url'])
        data = req.content.decode()
        req.close()

        # Создаем файл в формате json с идентификатором вакансии в качестве названия
        # Записываем в него ответ запроса и закрываем файл
        fileName = r'C:\Users\Asus\Documents\GitHub\OTUS-2020-2021\{}.json'.format(v['id'])
        f = open(fileName, mode='w', encoding='utf8')
        f.write(data)
        f.close()

        time.sleep(0.25)

print('Вакансии собраны')
