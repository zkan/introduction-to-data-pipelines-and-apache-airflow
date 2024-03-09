import pandas as pd


df = pd.read_csv("titanic.csv")
print(df.head())

# วิธีการเรียกดูค่าใน Column
# df["Survived"]
# df.Survived

df.info()

age_not_null = df.Age.notnull()
dq_age = age_not_null.sum() / len(age_not_null)
print(f"Data Quality of Age: {dq_age}")

cabin_not_null = df.Cabin.notnull()
dq_cabin = cabin_not_null.sum() / len(cabin_not_null)
print(f"Data Quality of Cabin: {dq_cabin}")

embarked_not_null = df.Embarked.notnull()
dq_embarked = embarked_not_null.sum() / len(embarked_not_null)
print(f"Data Quality of Embarked: {dq_embarked}")

print(f"Completeness: {(dq_age + dq_cabin + dq_embarked) / 3}")