import csv
import random
import string
import os

names = ["Александр", "Алексей", "Анатолий", "Андрей", "Антон",
                   "Аркадий", "Арсений", "Артём", "Богдан", "Борис",
                   "Вадим", "Валентин", "Валерий", "Василий", "Виктор",
                   "Виталий", "Влад", "Владимир", "Вячеслав", "Георгий",
                   "Глеб", "Григорий", "Даниил", "Денис", "Дмитрий",
                   "Евгений", "Егор", "Иван", "Игорь", "Илья",
                   "Кирилл", "Константин", "Лев", "Леонид", "Максим",
                   "Марат", "Матвей", "Михаил", "Никита", "Николай",
                   "Олег", "Павел", "Пётр", "Ринат", "Роман",
                   "Руслан", "Сергей", "Станислав", "Тарас", "Тимофей",
                   "Фёдор", "Филипп", "Юрий", "Яков", "Ян",
                   "Ярослав", "Алекс", "Альберт", "Антонин", "Артур",
                   "Бронислав", "Вениамин", "Витольд", "Владислав", "Геннадий",
                   "Георгий", "Герман", "Давид", "Донат", "Дорофей",
                   "Ефим", "Захар", "Иван", "Изяслав", "Казимир",
                   "Ким", "Леонтий", "Матвиенко", "Мирослав", "Нестор",
                   "Олегарх", "Платон", "Равиль", "Радий", "Ростислав",
                   "Рудольф", "Савва", "Соломон", "Степан", "Фадей",
                   "Федосей", "Эдуард", "Эмиль", "Юлиан", "Юлий"]

surnames = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнов",
                      "Егоров", "Макаров", "Николаев", "Соловьев", "Волков",
                      "Лебедев", "Козлов", "Новиков", "Морозов", "Кравцов",
                      "Орлов", "Романов", "Мельников", "Ткачев", "Жуков",
                      "Алексеев", "Кудрявцев", "Белов", "Федоров", "Комаров",
                      "Киселев", "Некрасов", "Юдин", "Бобылев", "Зайцев",
                      "Сахаров", "Шилов", "Буров", "Королев", "Мухин",
                      "Калинин", "Ларионов", "Воробьев", "Фомин", "Беляев",
                      "Федосеев", "Маслов", "Коновалов", "Исаев", "Чернов",
                      "Третьяков", "Денисов", "Карпов", "Степанов", "Дмитриев",
                      "Гусев", "Максимов", "Крылов", "Куликов", "Назаров",
                      "Воронцов", "Рябинин", "Савин", "Михайлов", "Фокин",
                      "Андреев", "Лихачёв", "Родионов", "Лазарев", "Соколов",
                      "Григорьев", "Попов", "Анисимов", "Ершов", "Макеев",
                      "Гаврилов", "Тихомиров", "Кондратьев", "Акимов", "Аксёнов",
                      "Быков", "Лялин", "Панов", "Болдырев", "Данилов",
                      "Верещагин", "Копылов", "Симонов", "Дорофеев", "Терентьев",
                      "Киреев", "Щербаков", "Трофимов", "Мартынов", "Емельянов",
                      "Юрьев", "Рутковский"]

patronymics = ["Александрович", "Сергеевич", "Дмитриевич", "Петрович", "Владимирович",
                         "Иванович", "Андреевич", "Николаевич", "Олегович", "Геннадиевич",
                         "Михайлович", "Евгеньевич", "Федорович", "Алексеевич", "Ярославович",
                         "Георгиевич", "Степанович", "Юрьевич", "Юдинович", "Данилович",
                         "Владиславович", "Наумович", "Савелиевич", "Артёмович", "Романович",
                         "Давидович", "Тимофеевич", "Русланович", "Вячеславович", "Анатольевич",
                         "Матвеевич", "Виталиевич", "Григорьевич", "Васильевич", "Семёнович",
                         "Станиславович", "Леонидович", "Игоревич", "Павлович", "Яковлевич",
                         "Венедиктович", "Платонович", "Зиновьевич", "Елизарович", "Кузьмич",
                         "Петухович", "Афанасьевич", "Бориславич", "Демидович", "Викентьевич",
                         "Венцеславович", "Жданович", "Лазаревич", "Кириллович", "Иоаннович",
                         "Демьянович", "Родионович", "Русландирович", "Софониевич", "Лукьянович",
                         "Федосьевич", "Константинович", "Гаврилович", "Радиславович", "Тихонович",
                         "Фадеевич", "Герасимович", "Данильевич", "Лаврентьевич", "Назарьевич",
                         "Глебович", "Казимирович", "Исидорович", "Сергеевич", "Емельянович",
                         "Леонтевич", "Никонович", "Парамонович", "Кузьминич", "Ильич",
                         "Львович", "Ермакович", "Валериевич", "Савинич", "Агафонович",
                         "Артамонович", "Добрынич", "Леопольдович", "Онуфриевич", "Юринич",
                         "Арсентьевич", "Венцеславович", "Генрихович", "Альбертович", "Ильясович",
                         "Кимович", "Михеевич", "Рустамович", "Эдуардович", "Юрьевич"]
number_of_rows = 1000000
# Генерация случайного ФИО
def generate_full_name():
    surname = random.choice(surnames)
    name = random.choice(names)
    patronymic = random.choice(patronymics)
    return surname, name, patronymic

# Генерация случайного номера телефона
def generate_phone():
    return ''.join(random.choices(string.digits, k=10))

# Генерация случайного адреса
streets = ['Lenina', 'Pushkina', 'Gagarina', 'Sovetskaya', 'Pobedy']
cities = ['Moscow', 'Saint Petersburg', 'Kazan', 'Ekaterinburg', 'Novosibirsk']
def generate_address():
    street = random.choice(streets)
    city = random.choice(cities)
    return street, city

# Генерация случайных данных для csv файла
data = []
for _ in range(number_of_rows):  # генерируем 1000 строк
    surname, name, patronymic = generate_full_name()
    phone = generate_phone()
    street, city = generate_address()
    series_passport = ''.join(random.choices(string.digits, k=4))
    number_passport = ''.join(random.choices(string.digits, k=6))
    birthday = f"{random.randint(1, 28)}.{random.randint(1, 12)}.{random.randint(1950, 2000)}"
    created_dt = f"{random.randint(1, 28)}.{random.randint(1, 12)}.{random.randint(2020, 2022)}"
    updated_dt = f"{random.randint(1, 28)}.{random.randint(1, 12)}.{random.randint(2020, 2022)}"
    
    data.append([surname, name, patronymic, phone, street, city, series_passport, number_passport, birthday, created_dt, updated_dt])

filename = f'tuning/input/csv_{number_of_rows}/data.csv'
os.makedirs(os.path.dirname(filename), exist_ok=True)
# Запись данных в csv файл
with open(filename, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['surname', 'name', 'patronymic', 'phone', 'street', 'city', 'series_passport', 'number_passport', 'birthday', 'created_dt', 'updated_dt'])
    for row in data:
        writer.writerow(row)
