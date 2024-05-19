import time
import random
from functools import partial

from deap import base, creator, tools, algorithms
from pyspark.sql import SparkSession

# Инициализация Spark Session
spark = SparkSession.builder.appName("OptimizeShufflePartitions").getOrCreate()

def evaluate_shuffle_partitions(partitions):
    partitions = int(partitions[0])
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    
    start_time = time.time()
    
    # Вставьте здесь ваш Spark Job
    # Например:
    df = spark.read.csv("path_to_your_data.csv", header=True, inferSchema=True)
    result = df.groupBy("some_column").count().collect()
    
    duration = time.time() - start_time
    return (duration,)

# Определите ваши параметры эволюционного алгоритма
POPULATION_SIZE = 10  # Размер популяции
GENERATIONS = 5  # Количество поколений
MUTATION_PROBABILITY = 0.2  # Вероятность мутации
CROSSOVER_PROBABILITY = 0.5  # Вероятность кроссовера
PARTITION_RANGE = (100, 10000)  # Диапазон значений для spark.sql.shuffle.partitions

# Создаем типы для алгоритма
creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)

toolbox = base.Toolbox()

# Генератор случайных значений для параметра spark.sql.shuffle.partitions
toolbox.register("attr_int", random.randint, *PARTITION_RANGE)

# Структура индивидуума
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_int, 1)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

# Операторы эволюции
toolbox.register("mate", tools.cxBlend, alpha=0.5)
toolbox.register("mutate", tools.mutPolynomialBounded, low=PARTITION_RANGE[0], up=PARTITION_RANGE[1], eta=0.5, indpb=0.1)
toolbox.register("select", tools.selTournament, tournsize=3)
toolbox.register("evaluate", evaluate_shuffle_partitions)

# Основной процесс эволюции
def main():
    random.seed(42)
    population = toolbox.population(n=POPULATION_SIZE)
    hof = tools.HallOfFame(1)  # Архив лучших решений
    
    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("avg", lambda x: sum(x) / len(x))
    stats.register("min", min)
    stats.register("max", max)

    algorithms.eaSimple(population, toolbox, cxpb=CROSSOVER_PROBABILITY, mutpb=MUTATION_PROBABILITY, ngen=GENERATIONS, 
                        stats=stats, halloffame=hof, verbose=True)
    
    print(f"Best solution found: {hof[0]} with fitness {hof[0].fitness.values[0]}")
    print(f"Optimal spark.sql.shuffle.partitions: {hof[0][0]}")

if __name__ == "__main__":
    main()
    spark.stop()
