# sql procedures are snippets of pre-prepared sql codes that are saved for the
# purpose of running them over and over again. They are one way to prevent sql
# injection attacks if they contain no variable elements.

# Demonstrates a mapping procedure.
# CREATE PROCEDURE MIMIC.map_icd9_to_icd10_am()
# BEGIN
# 	UPDATE MIMIC.DIAGNOSES_ICD_copy di
# 	JOIN MIMIC.D_ICD_DIAGNOSES_copy did
# 	ON did.ICD9_CODE = di.ICD9_CODE
# 	SET di.ICD10_AM_CODE = did.ICD10_AM_CODE;
# END

# Demonstrates a check and add column to table procedure.
# CREATE PROCEDURE MIMIC.map_icd9_to_icd10_am(IN target VARCHAR(255))
# BEGIN
# 	IF NOT EXISTS (
# 	    SELECT NULL
# 	    FROM INFORMATION_SCHEMA.COLUMNS
# 	    WHERE table_name = 'DIAGNOSES_ICD_copy'
# 	    AND table_schema = 'MIMIC'
# 	    AND column_name = 'ICD10_AM_CODE'
# 	    ) THEN
# 		ALTER TABLE DIAGNOSES_ICD_copy ADD ICD10_AM_CODE VARCHAR(50);
# 	END IF;
# END

# Combining the two together and adding a variable.
# CREATE PROCEDURE MIMIC.test(IN target VARCHAR(255))
# BEGIN
# 	SET @table_name = target;
# 	SET @command = concat('ALTER TABLE ',@table_name,' ADD ICD10_AM_CODE VARCHAR(50);');
# 	PREPARE stmt FROM @command;
#
# 	IF NOT EXISTS (
# 		SELECT NULL
# 		FROM INFORMATION_SCHEMA.COLUMNS
# 		WHERE table_name = @table_name
# 		AND table_schema = 'MIMIC'
# 	    AND column_name = 'ICD10_AM_CODE'
# 	    ) THEN
# 		EXECUTE stmt;
# 		DEALLOCATE PREPARE stmt;
# 	END IF;
#
# 	SET @command = concat('UPDATE ',@table_name,' target JOIN MIMIC.D_ICD_DIAGNOSES_copy s ON s.ICD9_CODE = t.ICD9_CODE SET target.ICD10_AM_CODE = source.ICD10_AM_CODE;');
# 	PREPARE stmt FROM @command;
# 	EXECUTE stmt;
# 	DEALLOCATE PREPARE stmt;
# END
