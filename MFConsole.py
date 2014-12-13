import random
import tkinter
from tkinter import ttk
from tkinter import messagebox

class App(object):

    def __init__(self):
        self.root = tkinter.Tk()
        self.style = ttk.Style()
        self.root.title('Master File Console')
        available_themes = self.style.theme_names()
        random_theme = random.choice(available_themes)
        self.style.theme_use(random_theme)


        


app = App()
app.root.mainloop()
