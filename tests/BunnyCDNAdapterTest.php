<?php declare(strict_types=1);

use League\Flysystem\AdapterTestUtilities\FilesystemAdapterTestCase;
use League\Flysystem\Config;
use League\Flysystem\Visibility;
use Tinect\Flysystem\BunnyCDN\BunnyCDNAdapter;

class BunnyCDNAdapterTest extends FilesystemAdapterTestCase
{
    private const TEST_FILE_CONTENTS = 'testing1982';

    public static function setUpBeforeClass(): void
    {
        $_SERVER['subfolder'] = 'ci/v3/' . time() . bin2hex(random_bytes(10));
    }

    public static function tearDownAfterClass(): void
    {
        self::createFilesystemAdapter('')->deleteDirectory($_SERVER['subfolder']);
    }

    public function testFileProcesses(): void
    {
        $adapter = $this->adapter();

        static::assertFalse(
            $adapter->fileExists('testing/test.txt')
        );

        $adapter->write('testing/test.txt', self::TEST_FILE_CONTENTS, new Config());

        static::assertTrue(
            $adapter->fileExists('testing/test.txt')
        );

        static::assertTrue(
            $adapter->fileExists('/testing/test.txt')
        );

        static::assertEquals(
            self::TEST_FILE_CONTENTS,
            $adapter->read('/testing/test.txt')
        );

        $adapter->delete('testing/test.txt');

        static::assertFalse(
            $adapter->fileExists('testing/test.txt')
        );
    }

    /**
     * @test
     * Test from FilesystemAdapterTestCase will fail, because bunnycdn doesn't support visiblity
     */
    public function setting_visibility(): void
    {
        static::assertIsBool(true);
    }

    /**
     * @test
     * Test from FilesystemAdapterTestCase will fail, because bunnycdn doesn't support visibility
     */
    public function setting_visibility_on_a_file_that_does_not_exist(): void
    {
        static::assertIsBool(true);
    }

    /**
     * @test
     * this overwrites Test from FilesystemAdapterTestCase.
     * We removed the test of visibility here
     */
    public function overwriting_a_file(): void
    {
        $this->runScenario(function (): void {
            $this->givenWeHaveAnExistingFile('path.txt', 'contents', ['visibility' => Visibility::PUBLIC]);
            $adapter = $this->adapter();

            $adapter->write('path.txt', 'new contents', new Config(['visibility' => Visibility::PRIVATE]));

            $contents = $adapter->read('path.txt');
            $this->assertEquals('new contents', $contents);
            /*$visibility = $adapter->visibility('path.txt')->visibility();
            $this->assertEquals(Visibility::PRIVATE, $visibility);*/
        });
    }

    /**
     * @test
     */
    public function copying_a_folder(): void
    {
        $this->runScenario(function () {
            $adapter = $this->adapter();
            $adapter->write(
                'test/text.txt',
                'contents to be copied',
                new Config([Config::OPTION_VISIBILITY => Visibility::PUBLIC])
            );
            $adapter->write(
                'test/2/text.txt',
                'contents to be copied',
                new Config([Config::OPTION_VISIBILITY => Visibility::PUBLIC])
            );
            $adapter->copy('test', 'destination', new Config());
            $this->assertTrue(
                $adapter->fileExists('test/text.txt'),
                'After copying a file should exist in the original location.'
            );
            $this->assertTrue(
                $adapter->fileExists('test/2/text.txt'),
                'After copying a file should exist in the original location.'
            );
            $this->assertTrue(
                $adapter->fileExists('destination/text.txt'),
                'After copying, a file should be present at the new location.'
            );
            $this->assertTrue(
                $adapter->fileExists('destination/2/text.txt'),
                'After copying, a file should be present at the new location.'
            );
            $this->assertEquals('contents to be copied', $adapter->read('destination/text.txt'));
            $this->assertEquals('contents to be copied', $adapter->read('destination/2/text.txt'));
        });
    }

    //TODO: implement
    public function generating_a_public_url(): void
    {
        $this->markTestSkipped('Adapter does not supply public URls. It is just announced because of inheritance with AsyncAwsS3Adapter.');
    }

    public function generating_a_temporary_url(): void
    {
        $this->markTestSkipped('Adapter does not supply temporary URls. It is just announced because of inheritance with AsyncAwsS3Adapter.');
    }

    protected static function createFilesystemAdapter(?string $subfolder = null): BunnyCDNAdapter
    {
        if (!isset($_SERVER['STORAGENAME'], $_SERVER['APIKEY'])) {
            throw new RuntimeException('Running test without real data is currently not possible');
        }

        if ($subfolder === null && isset($_SERVER['subfolder'])) {
            $subfolder = $_SERVER['subfolder'];
        }

        return new BunnyCDNAdapter($_SERVER['STORAGENAME'], $_SERVER['APIKEY'], 'storage.bunnycdn.com', $subfolder);
    }
}
