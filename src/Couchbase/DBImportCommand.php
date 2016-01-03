<?php

namespace MehrAlsNix\Couchbase;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\RuntimeException;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class DBImportCommand extends Command
{
    private static $COUCHBASE_URIS = "cb.uris";
    private static $COUCHBASE_BUCKET = "cb.bucket";
    private static $COUCHBASE_USERNAME = "cb.username";
    private static $COUCHBASE_PASSWORD = "cb.password";

    private static $SQL_DATABASE = "sql.database";
    private static $SQL_USER = "sql.username";
    private static $SQL_PASSWORD = "sql.password";

    private static $TABLES_LIST = "import.tables";
    private static $CREATE_VIEWS = "import.createViews";
    private static $TYPE_FIELD = "import.typefield";
    private static $TYPE_CASE = "import.fieldcase";

    /** @var \couchbaseCluster $couchbaseClient */
    private $couchbaseClient;

    /** @var Connection $connection */
    private $connection;

    private $sqlDatabase;
    private $sqlUser;
    private $sqlPassword;

    private $uris = [];
    private $bucket = 'default';
    private $defaultUri = 'http://127.0.0.1:8091/pools';
    private $password = "";
    private $username = "Administrator";

    private $typeField;
    private $typeFieldCase;
    private $createTableViewEnable = true;
    private $tableList;

    /** @var OutputInterface $output */
    private $output;

    protected function configure()
    {
        $this->setName('import')
            ->setDescription('Doctrine based Couchbase importer.')
            ->addArgument(
                'config',
                InputArgument::REQUIRED,
                'The location of the config file.'
            )
            ->addUsage('Set the location of the config.ini file.');
    }

    public function execute(InputInterface $input, OutputInterface $output)
    {
        $this->output = $output;

        $output->writeln("\n\n");
        $output->writeln("############################################");
        $output->writeln("#         COUCHBASE SQL IMPORTER           #");
        $output->writeln("############################################\n\n");

        try {
            $this->setup($input->getArgument('config'));
            $this->importData();
            $this->shutdown();


        } catch (RuntimeException $e) {
            $output->writeln($e->getTraceAsString());
        }

        $output->writeln("\n\n              FINISHED");
        $output->writeln("############################################");
        $output->writeln("\n\n");
    }

    /**
     * @param string $fileName
     */
    private function setup($fileName)
    {
        try {
            $prop = parse_ini_file($fileName, true);

            if (isset($prop[self::$COUCHBASE_URIS])) {
                $this->uris[] = explode(',', $prop[self::$COUCHBASE_URIS]);

            } else {
                $this->uris[] = $this->defaultUri;
            }

            if (isset($prop[self::$COUCHBASE_BUCKET])) {
                $this->bucket = $prop[self::$COUCHBASE_BUCKET];
            }

            if (isset($prop[self::$COUCHBASE_PASSWORD])) {
                $this->password = $prop[self::$COUCHBASE_PASSWORD];
            }

            if (isset($prop[self::$COUCHBASE_USERNAME])) {
                $this->username = $prop[self::$COUCHBASE_USERNAME];
            }

            if (isset($prop[self::$SQL_DATABASE])) {
                $this->sqlDatabase = $prop[self::$SQL_DATABASE];
            } else {
                throw new RuntimeException(" Doctrine Connection String not specified");
            }

            if (isset($prop[self::$SQL_USER])) {
                $this->sqlUser = $prop[self::$SQL_USER];
            } else {
                throw new RuntimeException(" Doctrine User not specified");
            }

            if (isset($prop[self::$SQL_PASSWORD])) {
                $this->sqlPassword = $prop[self::$SQL_PASSWORD];
            } else {
                throw new RuntimeException(" Doctrine Password not specified");
            }

            if (isset($prop[self::$TABLES_LIST])) {
                $this->tableList = explode(',', $prop[self::$TABLES_LIST]);
            }

            if (isset($prop[self::$CREATE_VIEWS])) {
                $this->createTableViewEnable = (boolean)$prop[self::$CREATE_VIEWS];
            }

            if (isset($prop[self::$TYPE_FIELD])) {
                $this->typeField = $prop[self::$TYPE_FIELD];
            }

            if (isset($prop[self::$TYPE_CASE])) {
                $this->typeFieldCase = $prop[self::$TYPE_CASE];
            }


            $this->output->writeln("\nImporting table(s)");
            $this->output->writeln("\tfrom : \t" . $this->sqlDatabase);
            $this->output->writeln("\tto : \t" . implode(', ', $this->uris) . " - " . $this->bucket);


        } catch (RuntimeException $e) {
            $this->output->writeln($e->getMessage() . "\n\n");
            exit(0);
        }

    }

    private function shutdown()
    {
        if ($this->connection !== null && $this->connection->isConnected()) {
            $this->connection->close();
        }
    }

    public function importData()
    {
        if ($this->tableList === null || strcasecmp($this->tableList[0], "ALL")) {
            $this->importAllTables();
        } else {
            foreach ($this->tableList as $table) {
                $this->importTable(trim($table));
            }
        }

        if ($this->createTableViewEnable) {
            $this->createTableViews();
        }
    }

    public function importAllTables()
    {
        $tableNames = $this->getConnection()->getSchemaManager()->listTableNames();

        foreach ($tableNames as $tableName) {
            $this->importTable($tableName);
        }
    }

    public function importTable($tableName)
    {
        $this->output->writeln("\n  Exporting Table : " . $tableName);
        $typeName = $this->getNamewithCase($tableName, $this->typeFieldCase);
        if ($this->createTableViewEnable) {
            $this->createViewsForPrimaryKey($tableName);
        }
        $preparedStatement = null;
        $selectSQL = "SELECT * FROM " . $tableName;

        try {

            $preparedStatement = $this->getConnection()->prepare($selectSQL);
            $preparedStatement->execute();

            $numRow = 0;
            $counter = 1;
            while ($rs = $preparedStatement->fetch()) {
                $map = [];

                foreach ($this->getConnection()->getSchemaManager()->listTableColumns($tableName) as $rsmd) {
                    $columnName = $this->getNamewithCase($rsmd->getName(), $this->typeFieldCase);
                    if (!isset($rs[$columnName])) {
                        continue;
                    }
                    $map[$columnName][] = $rs[$columnName];
                }

                if ($this->typeField != null && !empty($this->typeField)) {
                    $map[$this->typeField] = $typeName;
                }

                // use the rs number as key with table name
                $this->getCouchbaseClient()->openBucket()->upsert($typeName . ":" . $counter, json_encode($map));

                $numRow = $counter;
                $counter++;
            }
            $this->output->writeln("    " . $numRow . " records moved to Couchbase.");

        } catch (\Exception $e) {
            $this->output->writeln($e->getMessage());
        }
    }

    private function createViewsForPrimaryKey($tableName)
    {
        $typeName = $this->getNamewithCase($tableName, $this->typeFieldCase);

        try {
            $array = $this->getConnection()->getSchemaManager()->listTableForeignKeys($tableName);

            $mapFunction = '';
            $ifStatement = '';
            $emitStatement = '';

            $mapFunction .= "function (doc, meta) {\n";
            $mapFunction .= "  var idx = (meta.id).indexOf(\":\");\n";
            $mapFunction .= "  var docType = (meta.id).substring(0,idx); \n";

            if (!empty($array) && count($array) == 1) {
                $ifStatement .= "  if (meta.type == 'json' && docType == '";
                $ifStatement .= $typeName;
                $ifStatement .= "'  && doc.";
                $ifStatement .= $this->getNamewithCase($array[0]->getForeignColumns()[0], $this->typeFieldCase);
                $ifStatement .= " ){ \n";
                $emitStatement .= "    emit(doc." . $array[0]->getForeignColumns()[0] . ");";
            } elseif (!empty($array) && count($array) > 1) {
                $emitStatement .= "    emit([";
                $ifStatement .= "  if (meta.type == 'json' && docType == '";
                $ifStatement .= $typeName;
                $ifStatement .= "'  && ";

                for ($i = 0; $i < count($array); $i++) {
                    $emitStatement .= "doc." . $this->getNamewithCase($array[$i]->getForeignColumns()[0], $this->typeFieldCase);
                    $ifStatement .= "doc." . $this->getNamewithCase($array[$i]->getForeignColumns()[0], $this->typeFieldCase);
                    if ($i < (count($array) - 1)) {
                        $emitStatement .= ", ";
                        $ifStatement .= " && ";
                    }
                }
                $ifStatement .= " ){\n";
                $emitStatement .= "]);\n";
            }

            $mapFunction .= $ifStatement
                . $emitStatement
                . "  }\n"
                . "}\n";

            $this->output->writeln("\n\n Create Couchbase views for table " . $typeName);
            $viewName = "by_pk";
            $map = ["views" => [$viewName => ["map" => $mapFunction, "reduce" => '_count']]];
            $this->getCouchbaseClient()->openBucket($this->bucket)->manager()->upsertDesignDocument($tableName, $map);
        } catch (RuntimeException $e) {
            $this->output->writeln($e->getTraceAsString());  //To change body of catch statement use File | Settings | File Templates.
        } catch (\Exception $e) {
            $this->output->writeln($e->getTraceAsString());  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private function createTableViews()
    {
        $this->output->writeln("\n\n Create Couchbase views for 'types' ....");

        $viewName = "by_type";

        $mapFunction = [
            "views" => [
                $viewName => [
                    "map" => 'function (doc, meta) {
                        if (meta.type == "json") {
                            var idx = (meta.id).indexOf(":");
                            emit((meta.id).substring(0,idx));
                        }
                    }',
                    "reduce" => '_count'
                ]
            ]
        ];

        $this->getCouchbaseClient()
            ->openBucket($this->bucket)
            ->manager()
            ->upsertDesignDocument('all', $mapFunction);
    }

    /**
     * @return \CouchbaseCluster
     */
    public function getCouchbaseClient()
    {
        if ($this->couchbaseClient === null) {
            $this->couchbaseClient = new \CouchbaseCluster($this->uris[0], $this->username, $this->password);
            $this->couchbaseClient->openBucket($this->bucket);
        }
        return $this->couchbaseClient;


    }

    public function setCouchbaseClient(\CouchbaseCluster $couchbaseClient)
    {
        $this->couchbaseClient = $couchbaseClient;
    }

    /**
     * @return \Doctrine\DBAL\Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getConnection()
    {
        if ($this->connection == null) {
            $this->connection = DriverManager::getConnection(
                [
                    'driver' => 'pdo_mysql',
                    'user' => $this->sqlUser,
                    'password' => $this->sqlPassword,
                    'host' => 'localhost',
                    'dbname' => $this->sqlDatabase
                ]
            );
        }
        return $this->connection;
    }

    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;
    }

    private function getNamewithCase($tablename, $nameType)
    {
        $returnValue = $tablename;
        if (strcasecmp($nameType, 'lower')) {
            $returnValue = strtolower($tablename);
        } elseif (strcasecmp($nameType, 'upper')) {
            $returnValue = strtoupper($tablename);
        }
        return $returnValue;
    }
}
