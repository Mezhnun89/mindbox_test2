from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создание SparkSession
spark = SparkSession.builder \
    .appName("ProductCategoryPairs") \
    .getOrCreate()

# Создание исходных данных
products = spark.createDataFrame([
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D"),
], ["ProductID", "ProductName"])

categories = spark.createDataFrame([
    (1, "Category X"),
    (2, "Category Y"),
    (3, "Category Z"),
], ["CategoryID", "CategoryName"])

product_category = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 1),
    (3, 3),
], ["ProductID", "CategoryID"])

# Получение всех пар "Имя продукта - Имя категории"
product_category_pairs = products.join(product_category, "ProductID") \
    .join(categories, "CategoryID") \
    .select("ProductName", "CategoryName")

# Получение имен всех продуктов, у которых нет категорий
products_without_categories = products.join(product_category, "ProductID", "left_anti") \
    .select("ProductName")

# Вывод результатов
print("Все пары «Имя продукта – Имя категории»:")
product_category_pairs.show()

print("Имена продуктов без категорий:")
products_without_categories.show()

# Закрытие SparkSession
spark.stop()
