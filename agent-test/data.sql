declare
    i int;
begin
    i := 0;
    while(i < 100)
    loop
        i := i + 1;
        insert into DP_CI.DP_TAB_1(COL1,COL2) values(DP_CI.SEQ_BIG_TABLE.nextval,1);
        if mod(i,20) =0 then
            commit;
        end if;
    end loop;
    commit;
end;
/