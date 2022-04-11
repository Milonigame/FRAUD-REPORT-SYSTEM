create or replace procedure raws_dim_terminals  is
R Varchar2(100);
f_terminal varchar2(100); 
f_terminal_type varchar2(100);
f_city  varchar2(100);
f_address varchar2(100);
rn number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select terminal, terminal_type, city,address,  rownum ro from (
    select terminal, terminal_type, city, address,datee from raws order by datee )));
    for i in 1..rn loop
        select terminal,terminal_type, city , address
            into f_terminal, f_terminal_type, f_city , f_address
            from (select terminal, terminal_type, city , address, rownum ro from
                (select terminal, terminal_type, city , address, datee  from 
                    raws order by datee)) where ro = i;
        select case when exists (select * from DIM_terminals  where terminal_id = f_terminal) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO DIM_terminals  VALUES (
            f_terminal , f_terminal_type, f_city , f_address, null,null);
         else     
            update dim_terminals  set terminal_type = f_terminal_type, terminal_city  = f_city ,
            terminal_address = f_address where terminal_id = f_terminal;
        end if;
    end loop;
    commit;
end raws_dim_terminals ;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

create or replace procedure raws_dim_clients is
R Varchar2(100);
f_client varchar2(100); 
f_last_name varchar2(100);
f_first_name varchar2(100);
f_patronymic varchar2(100);
f_date_of_birth varchar2(100);
f_passport varchar2(100);
f_passport_valid_to varchar2(100);
f_phone varchar2(100);
rn number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select client, last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone, datee, rownum ro from (
    select client, last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone, datee from raws order by datee)));
    for i in 1..rn loop
        select client, last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone
            into f_client, f_last_name, f_first_name, f_patronymic, f_date_of_birth, f_passport, f_passport_valid_to, f_phone
            from (select client, last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone, rownum ro, datee from
                (select client, last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone, datee   from 
                    raws order by datee)) where ro = i;
        select case when exists (select * from DIM_CLIENTS where client_id = f_client) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO DIM_CLIENTS VALUES (
            f_client, f_last_name, f_first_name, f_patronymic, 
            to_date(f_date_of_birth,'mm/dd/yyyy'), f_passport, 
            to_date(f_passport_valid_to,'mm/dd/yyyy'), f_phone,null,null);
         else     
            update dim_clients set last_name = f_last_name, first_name = f_first_name,
            patrinymic = f_patronymic, date_of_birth = to_date(f_date_of_birth,'mm/dd/yyyy'), passport_num = f_passport,
            passport_valid_to = to_date(f_passport_valid_to,'mm/dd/yyyy'), phone = f_phone where client_id = f_client;
        end if;
    end loop;
    commit;
end raws_dim_clients;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

create or replace procedure raws_dim_accounts is
R Varchar2(100);
f_account varchar2(100); 
f_account_valid_to varchar2(100);
f_client varchar2(100);
rn number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select account, account_valid_to, client, rownum ro from (
    select account, account_valid_to, client, datee  from raws order by datee)));
    for i in 1..rn loop
        select account, account_valid_to, client
            into f_account, f_account_valid_to, f_client
            from (select account, account_valid_to, client, rownum ro from
                (select account, account_valid_to, client, datee  from 
                    raws order by datee)) where ro = i;
        select case when exists (select * from DIM_accounts where account_num = f_account) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO DIM_accounts VALUES (
           f_account, to_date(f_account_valid_to,'mm/dd/yyyy'), f_client ,null,null);
         else     
            update dim_accounts set valid_to =  to_date(f_account_valid_to,'mm/dd/yyyy'), client = f_client where account_num = f_account;
        end if;
    end loop;
    commit;
end raws_dim_accounts;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

create or replace procedure raws_dim_cards is
R Varchar2(100);
f_card varchar2(100); 
f_account varchar2(100);
rn number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select card, account, rownum ro from (
    select card, account,datee  from raws order by datee)));
    for i in 1..rn loop
        select card, account
            into f_card, f_account
            from (select card , account, rownum ro from
                (select card , account,datee from 
                    raws order by datee)) where ro = i;
        select case when exists (select * from DIM_cards where card_num = f_card) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO DIM_cards VALUES (
           f_card, f_account, null, null);
         else     
            update dim_cards set account_num = f_account where card_num=f_card ;
        end if;
    end loop;
    commit;
end raws_dim_cards;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

create or replace procedure raws_fact_transactions is
R Varchar2(100);
f_trans_id varchar2(100); 
f_datee varchar2(100);
f_card varchar2(100);
f_oper_type varchar2(100);
f_amount varchar2(100);
f_oper_result varchar2(100);
f_terminal varchar2(100);
rn number;
begin
    rn:=0;
    select max(ro)
    into rn
    from (select ro from (
    select trans_id, datee, card, oper_type, amount, oper_result , terminal ,  rownum ro from (
    select trans_id, datee, card, oper_type, amount, oper_result , terminal  from raws order by datee)));
    for i in 1..rn loop
        select trans_id, datee, card, oper_type, amount, oper_result , terminal
            into f_trans_id, f_datee, f_card, f_oper_type, f_amount, f_oper_result , f_terminal
            from (select trans_id, datee, card, oper_type, amount, oper_result , terminal , rownum ro from
                (select trans_id, datee, card, oper_type, amount, oper_result , terminal    from 
                    raws order by datee)) where ro = i;
        select case when exists (select * from fact_transactions where trans_id = f_trans_id) then 1 else 0 end 
        into R
        from dual;
        if R = 0    
            then INSERT INTO fact_transactions VALUES (
            f_trans_id,  to_date(f_datee,'dd/mm/yyyy HH24:MI:SS'), f_card, f_oper_type, 
           to_number(replace(f_amount, '.', ',')), f_oper_result , 
           f_terminal, null,null);
         else     
            update fact_transactions set trans_date = to_date(f_datee,'dd/mm/yyyy HH24:MI:SS'), card_num = f_card,
             oper_type = f_oper_type, amt = to_number(replace(f_amount, '.', ',')), oper_result = f_oper_result ,
            terminal  = f_terminal  where trans_id = f_trans_id;
        end if;
    end loop;
    commit;
end raws_fact_transactions;

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------------------------
-- Совершение операции при просроченном паспорте

create or replace procedure dims_fact_stg_report_passport is   
    BEGIN   
    insert into stg_report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
    select trans_date , passport_num , last_name||' '||first_name||' '||patrinymic , PHONE, 
    'Совершение операции при просроченном паспорте' , sysdate  from(
    select * from fact_transactions
    left join (select * from dim_cards  
        left join (select * from dim_accounts 
            left join dim_clients on client = client_id) using (account_num)) using (card_num)
    left join dim_terminals on terminal = terminal_id
    ORDER BY trans_date) where passport_valid_to < trans_date ; 
commit;      
END dims_fact_stg_report_passport;
    
  

-- Совершение операции при недействующем договоре
create or replace procedure dims_fact_stg_report_accounts is   
    BEGIN   
    insert into stg_report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
    select trans_date FRAUD_DT, passport_num passport, last_name||' '||first_name||' '||patrinymic FIO, PHONE, 
'Совершение операции при недействующем договоре' FRAUD_TYPE, sysdate REPORT_DT from(
    select * from fact_transactions
        left join (select * from dim_cards  
            left join (select * from dim_accounts 
                left join dim_clients on client = client_id) using (account_num)) using (card_num)
        left join dim_terminals on terminal = terminal_id
        ORDER BY trans_date) where valid_to < trans_date;
commit;       
END dims_fact_stg_report_accounts;


-- Совершение операции в разных городах в течение 1 часа

create or replace procedure dims_fact_stg_report_cityhour is   
    BEGIN   
    insert into stg_report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
SELECT * FROM (
    select case when terminal_city != nvl(lag(terminal_city) over (partition by card_num order by trans_date),'нет') and
    trans_date - nvl(lag(trans_date) over (partition by card_num order by trans_date), TO_DATE('31.12.1009', 'dd.mm.yyyy')) <= '0,0416666666666666666666666666666666666667'
    then 
        trans_date end FRAUD_DT, passport_num PASSPORT, last_name||' '||first_name||' '||patrinymic FIO, PHONE,
        'Совершение операции в разных городах в течение 1 часа' FRAUD_TYPE, sysdate REPORT_DT from(  
            select * from fact_transactions
                left join (select * from dim_cards  
                    left join (select * from dim_accounts 
                        left join dim_clients on client = client_id) using (account_num)) using (card_num)
                left join dim_terminals on terminal = terminal_id
                ORDER BY card_num, trans_date)) where FRAUD_DT is not null order by fraud_dt;
commit;       
END dims_fact_stg_report_cityhour;


 
-- Попытка подбора сумм. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. 
--Последняя операция (успешная) в такой цепочке считается мошеннической.

create or replace procedure dims_fact_stg_report_20min3o is   
    BEGIN   
    insert into stg_report(fraud_dt, passport, fio, phone, fraud_type, report_dt)
select * from (
    select case when (trans_date - nvl(lag(trans_date,1) over (partition by card_num order by trans_date), TO_DATE('31.12.1009', 'dd.mm.yyyy'))) +
    (nvl(lag(trans_date,1) over (partition by card_num order by trans_date), TO_DATE('31.12.1009', 'dd.mm.yyyy')) -
    nvl(lag(trans_date,2) over (partition by card_num order by trans_date), TO_DATE('31.12.1009', 'dd.mm.yyyy')))
    <= '0,0138888888888888888888888888888888888889' 
    and
    amt < nvl(lag(amt,1) over (partition by card_num order by trans_date), '0') and
    nvl(lag(amt,1) over (partition by card_num order by trans_date), '0') <
    nvl(lag(amt,2) over (partition by card_num order by trans_date), '0') and
    oper_result != 'Отказ' and
    nvl(lag(oper_result,1) over (partition by card_num order by trans_date), 'нет') = 'Отказ' and
    nvl(lag(oper_result,2) over (partition by card_num order by trans_date), 'нет') = 'Отказ'
    then trans_date end FRAUD_DT,
    Passport_num PASSPORT, last_name||' '||first_name||' '||patrinymic FIO, PHONE, 'Попытка подбора сумм.' FRAUD_TYPE, SYSDATE REPORT_DT
    from (
        select * from fact_transactions
             left join (select * from dim_cards  
             left join (select * from dim_accounts 
             left join dim_clients on client = client_id) using (account_num)) using (card_num)
             left join dim_terminals on terminal = terminal_id
                ORDER BY trans_date)) where FRAUD_DT is not null order by fraud_dt;
commit;       
END dims_fact_stg_report_20min3o;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Запускать этот блок процедур для загрузки данных в КМД и построения предварительного отчета по мощеническим операциям.
BEGIN
raws_DIM_TERMINALS;
raws_DIM_CLIENTS;
raws_DIM_ACCOUNTS;
raws_DIM_CARDS;
raws_fact_TRANSACTIONS;
dims_fact_stg_report_passport;
dims_fact_stg_report_accounts;
dims_fact_stg_report_cityhour;
dims_fact_stg_report_20min3o;
END;


SELECT* FROM DIM_TERMINALS;
SELECT * FROM DIM_TERMINALS_HIST;
SELECT* FROM DIM_CLIENTS;
SELECT * FROM DIM_CLIENTS_HIST;
SELECT* FROM DIM_ACCOUNTS;
SELECT * FROM DIM_ACCOUNTS_HIST;
SELECT* FROM DIM_CARDS;
SELECT * FROM DIM_CARDS_HIST;
SELECT* FROM fact_TRANSACTIONS;
SELECT * FROM fact_TRANSACTIONS_HIST;
SELECT * FROM stg_REPORT order by Fraud_dt;

