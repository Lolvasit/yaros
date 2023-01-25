from peewee import CharField, Model, SqliteDatabase, IntegrityError

database = SqliteDatabase("settings.sqlite3")


class BaseModel(Model):
    class Meta:
        database = database


class Setting(BaseModel):
    name = CharField(primary_key=True)
    value = CharField(default=None, null=True)

    def __repr__(self) -> str:
        return f"<Setting {self.name}:{self.value}>"

    class Meta:
        table_name = "settings"

    @classmethod
    def get_many(cls, names):
        lst = [cls.get_or_none(cls.name == name) for name in names]
        return [item.value if item else None for item in lst]

    @classmethod
    def set_many(cls, kwargs):
        for name, value in kwargs.items():
            try:
                cls.create(name=name, value=value)
            except IntegrityError:
                cls.update({cls.value:value}).where(cls.name == name).execute()
