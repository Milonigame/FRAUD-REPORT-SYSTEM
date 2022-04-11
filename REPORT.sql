--Процедура формирования финального отчёта (инкремент)------------------------------------------------------------------------------
create or replace procedure stg_report_report is
R number;
f_fraud_dt varchar2(100); 
f_passport varchar2(100);
f_fio  varchar2(100);
f_phone varchar2(100);
f_fraud_type varchar2(100);
f_report_dt varchar2(100);
rn number;
skip number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select fraud_dt, passport, fio, phone, fraud_type, report_dt, rownum ro from (
    select fraud_dt, passport, fio, phone, fraud_type, report_dt from stg_report order by fraud_dt)));
    for i in 1..rn loop
        skip:=0;
        select fraud_dt, passport, fio, phone, fraud_type, report_dt
            into f_fraud_dt, f_passport, f_fio , f_phone, f_fraud_type, f_report_dt
            from (select fraud_dt, passport, fio, phone, fraud_type, report_dt, rownum ro from (
                 select fraud_dt, passport, fio, phone, fraud_type, report_dt from stg_report order by fraud_dt)) where ro = i;
        select case when exists (select * from report where fraud_dt = f_fraud_dt and passport = f_passport and fio = f_fio and phone = f_phone and
        fraud_type = f_fraud_type) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO report  VALUES (
            f_fraud_dt, f_passport, f_fio , f_phone, f_fraud_type, sysdate);
         else     
            skip :=1;
        end if;
    end loop;
    commit;
end stg_report_report;

------------------------------------------------------------------------------


BEGIN
stg_report_report;
END;




SELECT * FROM REPORT order by Fraud_dt;




