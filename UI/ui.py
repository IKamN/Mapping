import tkinter as tk
from tkinter import ttk, filedialog
import yaml


def submit():
    # Чтение значений из полей
    values = {}
    for i in range(11):
        entry = entries[i]
        field_name = labels[i]
        value = entry.get()
        values[field_name] = value

    # Обновление файла config.yml с новыми значениями
    with open("/media/kini/B0E4EF45E4EF0C82/PythonFolder/parsing/VTB/Mapping/config.yml", "w", encoding="utf-8") as file:
        yaml.dump(values, file, allow_unicode=True, default_style="'")

    # subprocess.run(["python3", "main.py"])
    root.destroy()

root = tk.Tk()

screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Вычисление координат окна
window_width = 800
window_height = 300
x = (screen_width - window_width) // 2
y = (screen_height - window_height) // 2

# Установка геометрии окна
root.geometry(f"{window_width}x{window_height}+{x}+{y}")

# Создание названий полей
labels = [
    "file_dir",
    "file_name",
    "base_system_source",
    "base_system_target",
    "mapping_version",
    "docs",
    "developer",
    "id_is",
    "loadType",
    "database",
    "topic"
]

# Значения по умолчанию из файла config.yml
default_values = {}
try:
    with open("/media/kini/B0E4EF45E4EF0C82/PythonFolder/parsing/VTB/Mapping/config.yml", "r", encoding="utf-8") as file:
        default_values = yaml.safe_load(file)
except FileNotFoundError:
    pass

# 10 полей и заполнение значениями по умолчанию
entries = []
for i in range(11):
    label = tk.Label(root, text=labels[i])
    label.grid(row=i, column=0, sticky=tk.E)
    entry = tk.Entry(root, width=60)
    entry.grid(row=i, column=1)
    field_name = labels[i]
    entry.insert(0, default_values.get(field_name, ""))  # Установка значения по умолчанию из config.yml
    entries.append(entry)

# Поле выбора директории
def browse_directory():
    directory = filedialog.askdirectory()
    entry_directory.delete(0, tk.END)
    entry_directory.insert(0, directory)

label_directory = tk.Label(root, text="file_dir")
label_directory.grid(row=0, column=0, sticky=tk.E)
entry_directory = tk.Entry(root, width=50)
entry_directory.grid(row=0, column=1, columnspan=2)
button_browse = tk.Button(root, text="Browse", command=browse_directory)
button_browse.grid(row=0, column=3)

# Поле выбора типа загрузки
label_load_type = tk.Label(root, text="loadType")
label_load_type.grid(row=8, column=0, sticky=tk.E)
load_type_var = tk.StringVar(root)


# Кнопка отправки формы
submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.grid(row=14, columnspan=3)


root.mainloop()
