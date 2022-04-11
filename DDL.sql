Drop table Raws;
DROP TABLE FACT_TRANSACTIONS;
DROP TABLE FACT_TRANSACTIONS_HIST;
DROP TABLE DIM_TERMINALS;
DROP TABLE DIM_TERMINALS_HIST;
DROP TABLE DIM_CARDS;
DROP TABLE DIM_CARDS_HIST;
DROP TABLE DIM_ACCOUNTS;
DROP TABLE DIM_ACCOUNTS_HIST;
DROP TABLE DIM_CLIENTS;
DROP TABLE DIM_CLIENTS_HIST;
DROP TABLE stg_REPORT;
DROP TABLE REPORT;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--DIM_TABLES
DELETE FACT_TRANSACTIONS;
DELETE FACT_TRANSACTIONS_HIST;
DELETE DIM_TERMINALS;
DELETE DIM_TERMINALS_HIST;
DELETE DIM_CARDS;
DELETE DIM_CARDS_HIST;
DELETE DIM_ACCOUNTS;
DELETE DIM_ACCOUNTS_HIST;
DELETE DIM_CLIENTS;
DELETE DIM_CLIENTS_HIST;
DELETE stg_REPORT;
DELETE REPORT;
delete raws;

--------------------------------------------------------------------------------
--Если грузите данные через Python ,то создавайте эту таблицу Raws
--------------------------------------------------------------------------------
CREATE TABLE Raws(
TRANS_ID          VARCHAR2(100),
DATEE             VARCHAR2(100),      
CARD              VARCHAR2(100),
ACCOUNT           VARCHAR2(100),
ACCOUNT_VALID_TO  VARCHAR2(100),
CLIENT            VARCHAR2(100),
LAST_NAME         VARCHAR2(100),
FIRST_NAME        VARCHAR2(100),
PATRONYMIC        VARCHAR2(100),
DATE_OF_BIRTH     VARCHAR2(100),
PASSPORT          VARCHAR2(100),
PASSPORT_VALID_TO VARCHAR2(100),
PHONE             VARCHAR2(100),  
OPER_TYPE         VARCHAR2(100),
AMOUNT            VARCHAR2(100),
OPER_RESULT       VARCHAR2(100),
TERMINAL          VARCHAR2(100),
TERMINAL_TYPE     VARCHAR2(100),
CITY              VARCHAR2(100),
ADDRESS           VARCHAR2(100)
);


-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

--table 1
--drop table dim_terminals ;
create table dim_terminals 
(terminal_id  varchar(100) 
, terminal_type varchar(100),
terminal_city     varchar(100),
terminal_address  varchar(500),
create_dt    DATE,
update_dt DATE,
CONSTRAINT terminal_PK PRIMARY KEY (terminal_id));

--table2

--drop table dim_clients;
create table dim_clients (
client_id          varchar(100), 
last_name          varchar(100),
first_name         varchar(100),
patrinymic         varchar(100),
date_of_birth      date,         
passport_num       varchar(100),
passport_valid_to  date,
phone              varchar(100),
create_dt          date,
update_dt          date,

CONSTRAINT client_PK PRIMARY KEY (client_id)
);

--table3
--drop table DIM_accounts ;
create table   DIM_accounts (
account_num  varchar(100),
valid_to     date,
client       varchar(100),
create_dt    date,
update_dt    date,

CONSTRAINT account_PK PRIMARY KEY (account_num),
CONSTRAINT fk_client foreign key (client) references dim_clients(client_id)
);

--table4
--drop table  DIM_cards;
create table  DIM_cards (
card_num     varchar(100) ,
account_num  varchar(100),
create_dt    date,
update_dt    date,
CONSTRAINT card_PK PRIMARY KEY (card_num),
CONSTRAINT fk_account_num foreign key (account_num) references dim_accounts(account_num)
);

--table5
--drop table  FACT_transactions;
create table FACT_transactions (
trans_id    varchar(100),
trans_date  date,
card_num    varchar(100),
oper_type   varchar(100),
amt         decimal (10,2),
oper_result varchar(100),
terminal    varchar(100),
create_dt   date,
update_dt   date,

CONSTRAINT fk_card foreign key (card_num) references dim_cards(card_num),
CONSTRAINT fk_terminal foreign key (terminal) references dim_terminals(terminal_id)
);

--DIM_TABLES_HIST

--table 1
--drop table dim_terminals_hist ;
create table dim_terminals_hist 
(terminal_id      varchar(100) , terminal_type varchar(100),
terminal_city     varchar(100),
terminal_address  varchar(500),
start_dt          date,
end_dt            date
);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

--table2

--drop table dim_clients_hist ;
create table dim_clients_hist  (
client_id          varchar(100), 
last_name          varchar(100),
first_name         varchar(100),
patrinymic         varchar(100),
date_of_birth      date,         
passport_num       varchar(100),
passport_valid_to  date,
phone              varchar(100),
start_dt           date,
end_dt             date
);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

--table3
--drop table DIM_accounts_hist  ;
create table   DIM_accounts_hist  (
account_num  varchar(100),
valid_to     date,
client       varchar(100),
start_dt     date,
end_dt       date
);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

--table4
--drop table  DIM_cards_hist ;
create table  DIM_cards_hist  (
card_num       varchar(100) ,
account_num    varchar(100),
start_dt       date,
end_dt         date
);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

--table5
--drop table  FACT_transactions_hist ;
create table fact_transactions_hist  (
trans_id    varchar(100),
trans_date  date,
card_num    varchar(100),
oper_type   varchar(100),
amt         decimal,
oper_result varchar(100),
terminal    varchar(100),
start_dt    date,
end_dt      date

);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

create table stg_Report (
FRAUD_DT     date,
PASSPORT     varchar(100),
FIO          varchar(100),
PHONE        varchar(100),
FRAUD_TYPE   varchar(100),
REPORT_DT    date
);

-----------------------------------------------------------------------------  
----------------------------------------------------------------------------- 

create table Report (
FRAUD_DT     date,
PASSPORT     varchar(100),
FIO          varchar(100),
PHONE        varchar(100),
FRAUD_TYPE   varchar(100),
REPORT_DT    date
);

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_terminals BEFORE
    INSERT OR UPDATE ON dim_terminals
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        :new.create_dt := sysdate;
        :new.update_dt := TO_DATE('31.12.9999', 'dd.mm.yyyy');
    ELSIF updating THEN
        IF
            :old.terminal_id = :new.terminal_id
            AND :old.terminal_type = :new.terminal_type
            AND :old.terminal_city = :new.terminal_city
            AND :old.terminal_address = :new.terminal_address
        THEN
            skip := 1;
        ELSE
            :new.update_dt := sysdate;
        END IF;
    END IF;
END trig_dim_terminals;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 
CREATE OR REPLACE TRIGGER trig_dim_terminals_hist BEFORE
    INSERT OR UPDATE ON dim_terminals
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        INSERT INTO dim_terminals_hist (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            start_dt,
            end_dt
        ) VALUES (
            :new.terminal_id,
            :new.terminal_type,
            :new.terminal_city,
            :new.terminal_address,
            sysdate,
            TO_DATE('31.12.9999', 'dd.mm.yyyy')
        );
    ELSIF updating THEN
        IF
            :old.terminal_id = :new.terminal_id
            AND :old.terminal_type = :new.terminal_type
            AND :old.terminal_city = :new.terminal_city
            AND :old.terminal_address = :new.terminal_address
        THEN
            skip := 1;
        ELSE
            UPDATE dim_terminals_hist
            SET
                end_dt = sysdate
            WHERE
                    terminal_id = :new.terminal_id
                AND end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy');
            INSERT INTO dim_terminals_hist (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                start_dt,
                end_dt
            ) VALUES (
                :new.terminal_id,
                :new.terminal_type,
                :new.terminal_city,
                :new.terminal_address,
                sysdate,
                TO_DATE('31.12.9999', 'dd.mm.yyyy')
            );
        END IF;
    END IF;
END trig_dim_terminals_hist;


----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_clients BEFORE
    INSERT OR UPDATE ON dim_clients
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        :new.create_dt := sysdate;
        :new.update_dt := TO_DATE('31.12.9999', 'dd.mm.yyyy');
    ELSIF updating THEN
        IF
            :old.client_id = :new.client_id
            AND :old.last_name = :new.last_name
            AND :old.first_name = :new.first_name
            AND :old.patrinymic = :new.patrinymic
            AND :old.date_of_birth = :new.date_of_birth
            AND :old.passport_num = :new.passport_num
            AND :old.passport_valid_to = :new.passport_valid_to
            AND :old.phone = :new.phone
        THEN
            skip := 1;
        ELSE
            :new.update_dt := sysdate;
        END IF;
    END IF;
END trig_dim_clients;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_clients_hist BEFORE
    INSERT OR UPDATE ON dim_clients
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        INSERT INTO dim_clients_hist (
            client_id,
            last_name,
            first_name,
            patrinymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            start_dt,
            end_dt
        ) VALUES (
            :new.client_id,
            :new.last_name,
            :new.first_name,
            :new.patrinymic,
            :new.date_of_birth,
            :new.passport_num,
            :new.passport_valid_to,
            :new.phone,
            sysdate,
            TO_DATE('31.12.9999', 'dd.mm.yyyy')
        );
    ELSIF updating THEN
        IF
            :old.client_id = :new.client_id
            AND :old.last_name = :new.last_name
            AND :old.first_name = :new.first_name
            AND :old.patrinymic = :new.patrinymic
            AND :old.date_of_birth = :new.date_of_birth
            AND :old.passport_num = :new.passport_num
            AND :old.passport_valid_to = :new.passport_valid_to
            AND :old.phone = :new.phone
        THEN
            skip := 1;
        ELSE
            UPDATE dim_clients_hist
            SET
                end_dt = sysdate
            WHERE
                    client_id = :new.client_id
                AND end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy');
            INSERT INTO dim_clients_hist (
                client_id,
                last_name,
                first_name,
                patrinymic,
                date_of_birth,
                passport_num,
                passport_valid_to,
                phone,
                start_dt,
                end_dt
            ) VALUES (
                :new.client_id,
                :new.last_name,
                :new.first_name,
                :new.patrinymic,
                :new.date_of_birth,
                :new.passport_num,
                :new.passport_valid_to,
                :new.phone,
                sysdate,
                TO_DATE('31.12.9999', 'dd.mm.yyyy')
            );
        END IF;
    END IF;
END trig_dim_clients_hist;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER TRIG_dim_accounts BEFORE INSERT OR UPDATE ON dim_accounts  FOR EACH ROW
declare 
skip number;
BEGIN
	if INSERTING then 
        :new.create_dt := SYSDATE;
        :new.update_dt := to_date('31.12.9999','dd.mm.yyyy');
	elsif UPDATING then 
        if :OLD.account_num=:NEW.account_num AND :OLD.valid_to=:NEW.valid_to AND :OLD.client=:NEW.client
            then skip:=1;
            ELSE
            :new.update_dt := SYSDATE;
        end if;
	end if;
end TRIG_dim_accounts;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_accounts_hist BEFORE
    INSERT OR UPDATE ON dim_accounts
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        INSERT INTO dim_accounts_hist (
            account_num,
            valid_to,
            client,
            start_dt,
            end_dt
        ) VALUES (
            :new.account_num,
            :new.valid_to,
            :new.client,
            sysdate,
            TO_DATE('31.12.9999', 'dd.mm.yyyy')
        );
    ELSIF updating THEN
        IF
            :old.account_num = :new.account_num
            AND :old.valid_to = :new.valid_to
            AND :old.client = :new.client
        THEN
            skip := 1;
        ELSE
            UPDATE dim_accounts_hist
            SET
                end_dt = sysdate
            WHERE
                    account_num = :new.account_num
                AND end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy');
            INSERT INTO dim_accounts_hist (
                account_num,
                valid_to,
                client,
                start_dt,
                end_dt
            ) VALUES (
                :new.account_num,
                :new.valid_to,
                :new.client,
                sysdate,
                TO_DATE('31.12.9999', 'dd.mm.yyyy')
            );
        END IF;
    END IF;
END trig_dim_accounts_hist;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_cards BEFORE
    INSERT OR UPDATE ON dim_cards
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        :new.create_dt := sysdate;
        :new.update_dt := TO_DATE('31.12.9999', 'dd.mm.yyyy');
    ELSIF updating THEN
        IF
            :old.card_num = :new.card_num
            AND :old.account_num = :new.account_num
        THEN
            skip := 1;
        ELSE
            :new.update_dt := sysdate;
        END IF;
    END IF;
END trig_dim_cards;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_dim_cards_hist BEFORE
    INSERT OR UPDATE ON dim_cards
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        INSERT INTO dim_cards_hist (
            card_num,
            account_num,
            start_dt,
            end_dt
        ) VALUES (
            :new.card_num,
            :new.account_num,
            sysdate,
            TO_DATE('31.12.9999', 'dd.mm.yyyy')
        );
    ELSIF updating THEN
        IF
            :old.card_num = :new.card_num
            AND :old.account_num = :new.account_num
        THEN
            skip := 1;
        ELSE
            UPDATE dim_cards_hist
            SET
                end_dt = sysdate
            WHERE
                    card_num = :new.card_num
                AND end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy');
            INSERT INTO dim_cards_hist (
                card_num,
                account_num,
                start_dt,
                end_dt
            ) VALUES (
                :new.card_num,
                :new.account_num,
                sysdate,
                TO_DATE('31.12.9999', 'dd.mm.yyyy')
            );
        END IF;
    END IF;
END trig_dim_cards_hist;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_fact_transactions BEFORE
    INSERT OR UPDATE ON fact_transactions
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        :new.create_dt := sysdate;
        :new.update_dt := TO_DATE('31.12.9999', 'dd.mm.yyyy');
    ELSIF updating THEN
        IF
            :old.trans_id = :new.trans_id
            AND :old.trans_date = :new.trans_date
            AND :old.card_num = :new.card_num
            AND :old.oper_type = :new.oper_type
            AND :old.amt = :new.amt
            AND :old.oper_result = :new.oper_result
            AND :old.terminal = :new.terminal
        THEN
            skip := 1;
        ELSE
            :new.update_dt := sysdate;
        END IF;
    END IF;
END trig_fact_transactions;

----------------------------------------------------------------------------- 
----------------------------------------------------------------------------- 

CREATE OR REPLACE TRIGGER trig_fact_transactions_hist BEFORE
    INSERT OR UPDATE ON fact_transactions
    FOR EACH ROW
DECLARE
    skip NUMBER;
BEGIN
    IF inserting THEN
        INSERT INTO fact_transactions_hist (
            trans_id,
            trans_date,
            card_num,
            oper_type,
            amt,
            oper_result,
            terminal,
            start_dt,
            end_dt
        ) VALUES (
            :new.trans_id,
            :new.trans_date,
            :new.card_num,
            :new.oper_type,
            :new.amt,
            :new.oper_result,
            :new.terminal,
            sysdate,
            TO_DATE('31.12.9999', 'dd.mm.yyyy')
        );
    ELSIF updating THEN
        IF
            :old.trans_id = :new.trans_id
            AND :old.trans_date = :new.trans_date
            AND :old.card_num = :new.card_num
            AND :old.oper_type = :new.oper_type
            AND :old.amt = :new.amt
            AND :old.oper_result = :new.oper_result
            AND :old.terminal = :new.terminal
        THEN
            skip := 1;
        ELSE
            UPDATE fact_transactions_hist
            SET
                end_dt = sysdate
            WHERE
                    trans_id = :new.trans_id
                AND end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy');
            INSERT INTO fact_transactions_hist (
                trans_id,
                trans_date,
                card_num,
                oper_type,
                amt,
                oper_result,
                terminal,
                start_dt,
                end_dt
            ) VALUES (
                :new.trans_id,
                :new.trans_date,
                :new.card_num,
                :new.oper_type,
                :new.amt,
                :new.oper_result,
                :new.terminal,
                sysdate,
                TO_DATE('31.12.9999', 'dd.mm.yyyy')
            );
        END IF;
    END IF;
END trig_fact_transactions_hist;


select count(*) from raws