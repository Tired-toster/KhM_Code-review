create procedure syn.usp_ImportFileCustomerSeasonal 
	@ID_Record int
-- 1. Ключевые слова, названия системных функций и все операторы пишутся в нижнем регистре. Здесь использован верхний регистр
AS 
set nocount on
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	-- 2. При объявлении типов рекомендуется не использовать длину поля "max"
    declare @ErrorMessage varchar(max)
/*
	3. Комментарий должен быть с таким же отступом как и код, к которому он относится 
    Комментарий "Проверка на корректность загрузки" не отвечает этому требованию
*/
-- Проверка на корректность загрузки
	if not exists (
	select 1
	from syn.ImportFile as f
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		-- 4. "begin/end" должны быть на одном уровне с "if". В строках номер 22 и 27 номер это правило не соблюдено
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'
			raiserror(@ErrorMessage, 3, 1)
			-- 5. Перед "return" должна быть пустая строка
            return
		end

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		-- 6. Все виды "join"-ов в следующих строках до 57 включительно необходимо указать явно ("inner", "left")
        -- 7. Все виды  "join"  в следующих строках до 57 включительно необходимо написать с 1 отступом
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			/* 
				8. При наименовании объектов используется стиль PascalCase — во всех случаях, кроме схем 
				Однако в "c.ID_mapping_DataSource" это правило не соблюдено — "mapping" полностью в нижнем регистре
                Далее в коде это наменование с "mapping" встречается ещё несколько раз, 
                но я его не отмечал, так как обнаруженные ошиббки должны быть различными
			*/
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		/* 
			9. При наименовании алиаса необходимо использовать только первые заглавные буквы каждого слова в названии объекта, 
			которому дают алиас. Здесь же алиас назван "c_dist"
		*/
        join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	-- 10. Дополнительные условия переносятся на следующую строку с 1 отступом. То есть "and" должно быть с новой строки и отступом
    left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	-- 11. Перед названием таблицы, в которую осуществляется "merge", "into" не указывается
    merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched
		-- 12. "then" в конструкции "merge" записывается на одной строке с "when", независимо от наличия дополнительных условий
        and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		-- 13. При написании  "update" запроса, необходимо использовать конструкцию с "from" 
        update
		set ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
	
		-- 14. Длинные условия, формулы, выражения и т.п., занимающие более ~75% ширины экрана должны быть разделены на несколько строк
        values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)
		
        -- 15. Отсутствует пробел между двойным дефисом и первым словом комментария в следующей строке
		--Формирование таблицы для отчетности
		select top 100
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
            -- 16. Вероятно, пропущена точка в описании аргумента с алиасом "date": должно быть "bir.DateEnd", а не "birDateEnd"
            ,isnull(format(try_cast(birDateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		/* 
			17. "bir" является зарезервированным именем объекта, поэтому его нельзя использовать как алиас 
			Алиас "bir" необходимо переименовать, добавив первую согласную букву после заглавной из первого слова: "bdir"
		*/
        from #BadInsertedRows as bir

		return
	end
end
