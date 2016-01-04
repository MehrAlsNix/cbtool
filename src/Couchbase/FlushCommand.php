<?php
/**
 * cbtool.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * @copyright 2015 MehrAlsNix (http://www.mehralsnix.de)
 * @license   http://www.opensource.org/licenses/mit-license.php MIT
 *
 * @link      http://github.com/MehrAlsNix/cbtool
 */
namespace MehrAlsNix\Couchbase;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\RuntimeException;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class FlushCommand extends Command
{
    private static $COUCHBASE_URIS = "cb.uris";
    private static $COUCHBASE_BUCKET = "cb.bucket";
    private static $COUCHBASE_USERNAME = "cb.username";
    private static $COUCHBASE_PASSWORD = "cb.password";

    /** @var \couchbaseCluster $couchbaseClient */
    private $couchbaseClient;

    private $uris = [];
    private $bucket = 'default';
    private $defaultUri = 'http://127.0.0.1:8091/pools';
    private $password = "";
    private $username = "Administrator";

    /** @var OutputInterface $output */
    private $output;

    protected function configure()
    {
        $this->setName('flush')
            ->setDescription('Flush a couchbase bucket.')
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

        try {
            $this->setup($input->getArgument('config'));
            $this->getCouchbaseClient()->openBucket($this->bucket)->manager()->flush();
        } catch (\Exception $e) {
            $output->writeln($e->getTraceAsString());
        }
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

            $this->output->writeln("\nFlushed bucket : \t" . implode(', ', $this->uris) . " - " . $this->bucket);
        } catch (RuntimeException $e) {
            $this->output->writeln($e->getMessage() . "\n\n");
            exit(0);
        }
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
}
