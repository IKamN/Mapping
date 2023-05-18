import tkinter as tk
from tkinter import filedialog
import yaml



class Config:
    pass

config = Config()

labels = [
    "file_dir",
    "subo_name",
    "id_ris",
    "mapping_version",
    "system_target",
    "loadType",
    "docs",
    "developer",
    "database",
    "topic"
]


def get_interface(config_path):

    def finish_work():
        root.destroy()

    def submit():
        values = {}
        for i in range(10):
            entry = entries[i]
            field_name = labels[i]
            value = entry.get()
            values[field_name] = value

        for key, value in values.items():
            setattr(config, key, value)

        with open(config_path, "w", encoding="utf-8") as file:
            yaml.dump(values, file, allow_unicode=True, default_style="'")
        root.destroy()


    root = tk.Tk()
    root.title("Autogen")
    window_width = 650
    window_height = 280
    root.geometry(f"{window_width}x{window_height}+{210}+{210}")
    root.resizable(False, False)
    # icon = tk.PhotoImage(file = "")
    root.config(bg="#C8FDF1")

    default_values = {}
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            default_values = yaml.safe_load(file)
    except FileNotFoundError:
        print("File config.yml not found")

    len_config = len(default_values)

    entries = []
    for i in range(0, len_config):
        label = tk.Label(root, text=labels[i], font=("Arial", 12), bg="#C8FDF1")
        label.grid(row=i, column=0, stick="E", padx=10, sticky="wens")
        entry = tk.Entry(root, width=50)
        entry.grid(row=i, column=1, ipadx = 8, sticky="wens")
        field_name = labels[i]
        entry.insert(0, default_values.get(field_name, ""))
        entries.append(entry)

    # Button "Browse"
    button_browse = tk.Button(root, text="...", command= lambda: entry.insert(1, filedialog.askdirectory()))
    button_browse.grid(row=0, column=4, sticky="w")

    # Button "Create"
    create_button = tk.Button(root, text="Create", command=submit)
    create_button.grid(row=19, columnspan=3)

    root.mainloop()
    return config

# get_interface("/media/kini/B0E4EF45E4EF0C82/PythonFolder/parsing/VTB/Mapping/config.yml")

