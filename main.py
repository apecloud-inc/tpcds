import pymysql
import psycopg2
import os
import time
import argparse

drop_sql_path = './sql/drop/drop.sql'
create_table_path = './sql/create/create_table.sql'
create_table_ri_path = './sql/create/create_table_ri.sql'
load_data_path = './sql/load/load_pg.sql'

parser = argparse.ArgumentParser(description='TPC-DS Benchmark')
parser.add_argument('--scale', type=int, default=1, help='volume of data to generate in GB')
parser.add_argument('--driver', type=str, default='mysql', help='database driver to use, support mysql, postgres')
parser.add_argument('--host', type=str, default='localhost', help='database host')
parser.add_argument('--port', type=int, default=3306, help='database port')
parser.add_argument('--user', type=str, default='root', help='database user')
parser.add_argument('--password', type=str, default='root', help='database password')
parser.add_argument('--database', type=str, default='tpcds', help='database name')
parser.add_argument('--step', type=str, default='all', help='step to run, support cleanup, prepare, run, all')

def TPCDS_Cleanup(custor):
    print("-------------- TPCDS_Cleanup --------------")
    sqls = read_sql_file(drop_sql_path)
    for i, sql in enumerate(sqls):
        print("run drop table", i)
        print(sql.strip())
        custor.execute(sql)
        print("drop table", i, "success\n")
    
    custor.connection.commit()


def TPCDS_Prepare(custor, args):
    print("-------------- TPCDS_Prepare --------------")

    Gen_DATA(args.scale)

    if args.driver == 'mysql':
        Gen_Query_MySQL(args.scale)
    elif args.driver == 'postgres':
        Gen_Query_PG(args.scale)

    print("run tpcds create table")
    sqls = read_sql_file(create_table_path)
    for i, sql in enumerate(sqls):
        print("run create table", i)
        print(sql.strip())
        custor.execute(sql)
        print("create table", i, "success\n")

    print("run tpcds load data")
    if args.driver == 'mysql':
        mysql_load_data(custor)
    elif args.driver == 'postgres':
        pg_load_data(custor)

    # print("run tpcds create constraint and index")
    # for i, sql in enumerate(read_sql_file(create_table_ri_path)):
    #     print("run create constraint and index", i)
    #     print(sql.strip())
    #     custor.execute(sql)
    #     print("create constraint and index", i, "success\n")
    
    custor.connection.commit()


def TPCDS_Run(custor, args):
    print("-------------- TPCDS_Run --------------")
    custor.connection.autocommit = False

    for i in range(0, 99):
        print("run query", i + 1)
        with open(f'./cqueries/query{i+1}.sql', 'r') as f:
            sql = f.read()
            t1 = time.time()

            sqls = sql.split(';')
            for item in sqls:
                if item.strip() == '':
                    continue
                custor.execute(item)
            t2 = time.time()
            cost_time = t2 - t1
        print("query " + str(i+1) + " rows: " + str(len(custor.fetchall())))
        print("query " + str(i+1) + " run cost " + format(cost_time, '.3f') + " seconds")


def Gen_DATA(scale_factor : int):
    print("-------------- Gen_DATA --------------")
    if not os.path.exists('./data'):
        os.makedirs('./data')

    cmd = f'./tools/Linux/dsdgen -SCALE {scale_factor} -TERMINATE N -DIR ./data -FORCE'

    # run dsdgen
    os.system(cmd)


def Gen_Query_MySQL(scale_factor : int):
    print("-------------- Gen_Query_MySQL --------------")
    cmd = f'./tools/Linux/dsqgen -DIRECTORY ./query_templates -INPUT ./query_templates/templates_mysql.lst -VERBOSE Y -QUALIFY Y -SCALE {scale_factor} -DIALECT netezza -OUTPUT_DIR .'

    # run dsqgen
    os.system(cmd)

    # fix query error
    os.system('python3 sql_fix_mysql.py')

def Gen_Query_PG(scale_factor : int):
    print("-------------- Gen_Query_PG --------------")
    cmd = f'./tools/Linux/dsqgen -DIRECTORY ./query_templates -INPUT ./query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE {scale_factor} -DIALECT netezza -OUTPUT_DIR .'

    # run dsqgen
    os.system(cmd)

    # fix query error
    os.system('python3 sql_fix_pg.py')


def mysql_load_data(custor):
    # enable local_infile
    custor.execute("SET GLOBAL local_infile=1")
    custor.connection.commit()

    # call_center.dat
    print("load call_center.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/call_center.dat' INTO TABLE call_center FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load call_center.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # catalog_page.dat
    print("load catalog_page.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/catalog_page.dat' INTO TABLE catalog_page FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load catalog_page.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # catalog_returns.dat
    print("load catalog_returns.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/catalog_returns.dat' INTO TABLE catalog_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load catalog_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # catalog_sales.dat
    print("load catalog_sales.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/catalog_sales.dat' INTO TABLE catalog_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load catalog_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # customer.dat
    print("load customer.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/customer.dat' INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load customer.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # customer_address.dat
    print("load customer_address.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/customer_address.dat' INTO TABLE customer_address FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load customer_address.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # customer_demographics.dat
    print("load customer_demographics.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/customer_demographics.dat' INTO TABLE customer_demographics FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load customer_demographics.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # date_dim.dat
    print("load date_dim.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/date_dim.dat' INTO TABLE date_dim FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load date_dim.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # household_demographics.dat
    print("load household_demographics.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/household_demographics.dat' INTO TABLE household_demographics FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load household_demographics.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # income_band.dat
    print("load income_band.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/income_band.dat' INTO TABLE income_band FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load income_band.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # inventory.dat
    print("load inventory.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/inventory.dat' INTO TABLE inventory FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load inventory.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # item.dat
    print("load item.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/item.dat' INTO TABLE item FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load item.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # promotion.dat
    print("load promotion.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/promotion.dat' INTO TABLE promotion FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load promotion.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # reason.dat
    print("load reason.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/reason.dat' INTO TABLE reason FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load reason.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # ship_mode.dat
    print("load ship_mode.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/ship_mode.dat' INTO TABLE ship_mode FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load ship_mode.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # store.dat
    print("load store.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/store.dat' INTO TABLE store FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load store.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # store_returns.dat
    print("load store_returns.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/store_returns.dat' INTO TABLE store_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load store_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # store_sales.dat
    print("load store_sales.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/store_sales.dat' INTO TABLE store_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load store_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # time_dim.dat
    print("load time_dim.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/time_dim.dat' INTO TABLE time_dim FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load time_dim.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # warehouse.dat
    print("load warehouse.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/warehouse.dat' INTO TABLE warehouse FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load warehouse.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # web_page.dat
    print("load web_page.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/web_page.dat' INTO TABLE web_page FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load web_page.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_returns.dat
    print("load web_returns.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/web_returns.dat' INTO TABLE web_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load web_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # web_sales.dat
    print("load web_sales.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/web_sales.dat' INTO TABLE web_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load web_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_site.dat
    print("load web_site.dat")
    t1 = time.time()
    custor.execute("LOAD DATA LOCAL INFILE './data/web_site.dat' INTO TABLE web_site FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'")
    t2 = time.time()
    print("load web_site.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    

def pg_load_data(custor):
    # call_center.dat
    print("load call_center.dat")
    t1 = time.time()
    with open('./data/call_center.dat', 'r', encoding='latin-1') as f:
        custor.copy_expert("COPY call_center FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load call_center.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # catalog_page.dat
    print("load catalog_page.dat")
    t1 = time.time()
    with open('./data/catalog_page.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY catalog_page FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load catalog_page.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # catalog_returns.dat
    print("load catalog_returns.dat")
    t1 = time.time()
    with open('./data/catalog_returns.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY catalog_returns FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load catalog_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # catalog_sales.dat
    print("load catalog_sales.dat")
    t1 = time.time()
    with open('./data/catalog_sales.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY catalog_sales FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load catalog_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # customer.dat
    print("load customer.dat")
    t1 = time.time()
    with open('./data/customer.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY customer FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load customer.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # customer_address.dat
    print("load customer_address.dat")
    t1 = time.time()
    with open('./data/customer_address.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY customer_address FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load customer_address.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # customer_demographics.dat
    print("load customer_demographics.dat")
    t1 = time.time()
    with open('./data/customer_demographics.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY customer_demographics FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load customer_demographics.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # date_dim.dat
    print("load date_dim.dat")
    t1 = time.time()
    with open('./data/date_dim.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY date_dim FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load date_dim.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # household_demographics.dat
    print("load household_demographics.dat")
    t1 = time.time()
    with open('./data/household_demographics.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY household_demographics FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load household_demographics.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # income_band.dat
    print("load income_band.dat")
    t1 = time.time()
    with open('./data/income_band.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY income_band FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load income_band.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # inventory.dat
    print("load inventory.dat")
    t1 = time.time()
    with open('./data/inventory.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY inventory FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load inventory.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # item.dat
    print("load item.dat")
    t1 = time.time()
    with open('./data/item.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY item FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load item.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")

    # promotion.dat
    print("load promotion.dat")
    t1 = time.time()
    with open('./data/promotion.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY promotion FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load promotion.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # reason.dat
    print("load reason.dat")
    t1 = time.time()
    with open('./data/reason.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY reason FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load reason.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # ship_mode.dat
    print("load ship_mode.dat")
    t1 = time.time()
    with open('./data/ship_mode.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY ship_mode FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load ship_mode.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # store.dat
    print("load store.dat")
    t1 = time.time()
    with open('./data/store.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY store FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load store.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # store_returns.dat
    print("load store_returns.dat")
    t1 = time.time()
    with open('./data/store_returns.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY store_returns FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load store_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # store_sales.dat
    print("load store_sales.dat")
    t1 = time.time()
    with open('./data/store_sales.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY store_sales FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load store_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # time_dim.dat
    print("load time_dim.dat")
    t1 = time.time()
    with open('./data/time_dim.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY time_dim FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load time_dim.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # warehouse.dat
    print("load warehouse.dat")
    t1 = time.time()
    with open('./data/warehouse.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY warehouse FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load warehouse.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_page.dat
    print("load web_page.dat")
    t1 = time.time()
    with open('./data/web_page.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY web_page FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load web_page.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_returns.dat
    print("load web_returns.dat")
    t1 = time.time()
    with open('./data/web_returns.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY web_returns FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load web_returns.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_sales.dat
    print("load web_sales.dat")
    t1 = time.time()
    with open('./data/web_sales.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY web_sales FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load web_sales.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")
    
    # web_site.dat
    print("load web_site.dat")
    t1 = time.time()
    with open('./data/web_site.dat', encoding='latin-1') as f:
        custor.copy_expert("COPY web_site FROM STDIN WITH CSV DELIMITER '|' NULL AS ''", f)
    t2 = time.time()
    print("load web_site.dat success, cost " + format(t2 - t1, '.3f') + " seconds\n")


def read_sql_file(file_path : str) -> list:
    """
    Read an SQL file and return a list of SQL statements

    Args:
    file_path (str): The path to the SQL file.

    Returns:
    list: A list of SQL statements.

    """

    with open(file_path, 'r') as file:
        data = file.read()
    # Split file into separate SQL statements
    sql_statements = data.split(';')
    # Remove any empty statements and add a semicolon to the end of each SQL statement
    sql_statements = [f"{statement.strip()};\n" for statement in sql_statements if statement.strip() != '']
    return sql_statements

def main():
    args = parser.parse_args()

    if args.driver == 'mysql':
        conn = pymysql.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            charset='utf8',
            local_infile=True
        )
        custor = conn.cursor()
    elif args.driver == 'postgres':
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        custor = conn.cursor()

    if args.step == 'cleanup' or args.step == 'all':
        TPCDS_Cleanup(custor)
    
    if args.step == 'prepare' or args.step == 'all':
        TPCDS_Prepare(custor, args)
    
    if args.step == 'run' or args.step == 'all':
        TPCDS_Run(custor, args)

    # close connection
    custor.close()
    conn.close()


if __name__ == "__main__":
    main()
