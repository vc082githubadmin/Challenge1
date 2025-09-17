#Assume source table is DB.SCHEMA.TABLE
        db, sch, tab = [x.strip('"') for x in source_table.split('.')]
        
        obj_pairs_sql = f"""
            select listagg(
                '''' || column_name || ''', "' || column_name || '"', ', '
            ) within group (order by ordinal_position) as obj_pairs
            from {db}.information_schema.columns
            where table_schema = '{sch}' and table_name = '{tab}';
        """
        obj_pairs = session.sql(obj_pairs_sql).collect()[0]['OBJ_PAIRS']
        
        # create a view with a null-safe, order-stable fingerprint over all columns
        create_fp_view_sql = f"""
            create or replace view {db}.{sch}.v_{tab}_with_fp as 
            select t.*,
                HASH(TO_JSON(object_construct_keep_null({obj_pairs}))) as rec_fp
            from {source_table} t
        """
        session.sql(create_fp_view_sql).collect()
