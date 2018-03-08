import loader


# TODO:  These tests sorta depend on the presence of /data/2017-08-17/{foo,bar} being present.  This is a known
# deficiency

def test_load_config():
    config = loader.LoadConfig(table="foo", path='/data/{date}/foo', dates=['2017-08-17'])
    statements = config.generate_sql()
    assert len(statements) == 2
    assert statements[0] == 'TRUNCATE TABLE foo;'
    assert statements[1] == "COPY foo FROM LOCAL '/data/2017-08-17/foo' DELIMITER ',' SKIP 1 DIRECT;"


def test_file_spec():
    file_spec = loader.FileSpec(',', True)
    assert file_spec.formatted_statement() == "DELIMITER ',' SKIP 1 DIRECT"

def test_yaml():
    tables = loader.load_table_configs('test.yml', dates=['2017-08-17'])
    assert len(tables) == 2
    foo = tables[0]

    assert foo.table == 'foo'
    bar = tables[1]
    assert bar.table == 'bar'

def test_dates():
    start_date = '2017-08-01'
    end_date = None
    dates = loader.compute_dates(start_date, end_date)
    assert len(dates) == 1
    assert dates[0] == start_date

    end_date = '2017-08-15'
    dates = loader.compute_dates(start_date, end_date)
    assert len(dates) == 15
    assert dates[0] == start_date
    assert dates[-1] == end_date