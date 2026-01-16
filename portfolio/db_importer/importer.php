<?php
/**
 * Database Migration Utility
 * 
 * Scalable solution for migrating large datasets from structured CSV files 
 * to relational databases (MySQL/MariaDB). Features atomic transactions 
 * and prepared statements for security and performance.
 */

class DataImporter
{
    private $connection;
    private $targetTable;

    public function __construct($host, $dbName, $username, $password, $tableName)
    {
        $dsn = "mysql:host=$host;dbname=$dbName;charset=utf8mb4";
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ];

        try {
            $this->connection = new PDO($dsn, $username, $password, $options);
            $this->targetTable = $tableName;
        } catch (\PDOException $e) {
            error_log("Connection failed: " . $e->getMessage());
            throw new \Exception("Database connection error. Please check credentials.");
        }
    }

    /**
     * Executes the bulk import from a CSV source file.
     * Uses transactions to ensure data consistency.
     */
    public function executeImport($sourcePath)
    {
        if (!file_exists($sourcePath) || !is_readable($sourcePath)) {
            throw new \Exception("The source file does not exist or is not readable.");
        }

        if (($handle = fopen($sourcePath, "r")) !== FALSE) {
            $headers = fgetcsv($handle, 1000, ",");
            $columns = implode(", ", $headers);
            $placeholders = implode(", ", array_fill(0, count($headers), "?"));

            $query = "INSERT INTO {$this->targetTable} ($columns) VALUES ($placeholders)";
            $stmt = $this->connection->prepare($query);

            try {
                $this->connection->beginTransaction();
                $insertedRows = 0;

                while (($row = fgetcsv($handle, 1000, ",")) !== FALSE) {
                    $stmt->execute($row);
                    $insertedRows++;
                }

                $this->connection->commit();
                echo "Migration successful: $insertedRows records processed.\n";
            } catch (\Exception $e) {
                $this->connection->rollBack();
                error_log("Migration failed: " . $e->getMessage());
                throw $e;
            } finally {
                fclose($handle);
            }
        }
    }
}

// Example Initialization
/*
try {
    $importer = new DataImporter('db_host', 'inventory_db', 'db_user', 'db_pass', 'product_catalog');
    $importer->executeImport('import_inventory_2026.csv');
} catch (\Exception $e) {
    echo "Error: " . $e->getMessage();
}
*/
?>
