<?php
declare(strict_types=1);

namespace Tinect\Flysystem\BunnyCDN;

use AsyncAws\Core\Configuration;
use AsyncAws\S3\S3Client;
use League\Flysystem\AsyncAwsS3\AsyncAwsS3Adapter;
use League\Flysystem\CalculateChecksumFromStream;
use League\Flysystem\Config;
use League\Flysystem\DirectoryListing;
use League\Flysystem\FileAttributes;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\ExtensionMimeTypeDetector;
use League\MimeTypeDetection\FinfoMimeTypeDetector;

class BunnyCDNAdapter extends AsyncAwsS3Adapter
{
    use CalculateChecksumFromStream;

    /**
     * This is used in tests. Don't remove it!
     *
     * @var string[]
     */
    public const EXTRA_METADATA_FIELDS = [
        'Metadata',
        'StorageClass',
        'ETag',
        'Checksum', // TODO: check with bunny.net
        'VersionId',
    ];

    public function __construct(
        string $storageName,
        string $apiKey,
        string $endpoint,
        string $subfolder = ''
    )
    {
        if ($subfolder !== '') {
            $subfolder = rtrim($subfolder, '/') . '/';
        }

        if (!str_starts_with($endpoint, 'http')) {
            $endpoint = 'https://' . $endpoint;
        }

        $s3client = new S3Client([
            Configuration::OPTION_REGION => '',
            Configuration::OPTION_ENDPOINT => rtrim($endpoint, '/'),
            Configuration::OPTION_SEND_CHUNKED_BODY => false,
            Configuration::OPTION_ACCESS_KEY_ID => $storageName,
            Configuration::OPTION_SECRET_ACCESS_KEY => $apiKey,
            Configuration::OPTION_PATH_STYLE_ENDPOINT => true,
        ]);

        parent::__construct($s3client, $storageName, $subfolder, metadataFields: self::EXTRA_METADATA_FIELDS);
    }

    public function copy($source, $destination, Config $config): void
    {
        try {
            $files = $this->getFiles($source);

            $sourceLength = \strlen($source);

            foreach ($files as $file) {
                parent::copy($file, $destination.\substr($file, $sourceLength), $config);
            }
        } catch (UnableToReadFile|UnableToWriteFile $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }
    }

    public function move(string $source, string $destination, Config $config): void
    {
        if ($source === $destination) {
            return;
        }

        try {
            $files = $this->getFiles($source);

            $sourceLength = \strlen($source);

            foreach ($files as $file) {
                parent::move($file, $destination.\substr($file, $sourceLength), $config);
            }
        } catch (UnableToReadFile $e) {
            throw new UnableToMoveFile($e->getMessage());
        }
    }

    /**
     * BunnyCDN does not support deep listing S3 like, so we need to filter out directories
     */
    public function listContents(string $path = '', bool $deep = false): iterable
    {
        $contents = parent::listContents($path, $deep);

        foreach ($contents as $content) {
            yield $content;

            if ($deep === true && $content->isDir() === true) {
                foreach ($this->listContents($content->path(), true) as $innerContent) {
                    yield $innerContent;
                }
            }
        }
    }

    /*
     * TODO: check the reason. Time issue?
     */
    public function lastModified(string $path): FileAttributes
    {
        $result = parent::lastModified($path)->jsonSerialize();
        $result[StorageAttributes::ATTRIBUTE_LAST_MODIFIED] += 5;

        return FileAttributes::fromArray($result);
    }

    private function getFiles(string $source): iterable
    {
        $contents = $this->listContents($source, true);
        $hasElements = false;

        foreach ($contents as $entry) {
            if ($entry->isFile() === false) {
                continue;
            }

            $hasElements = true;

            yield $entry->path();
        }

        if ($hasElements === false) {
            yield $source;
        }
    }

    public function visibility(string $path): FileAttributes
    {
        if (!$this->fileExists($path)) {
            throw UnableToRetrieveMetadata::visibility($path);
        }

        return new FileAttributes($path, null, Visibility::PUBLIC);
    }

    public function setVisibility(string $path, string $visibility): void
    {
        throw UnableToSetVisibility::atLocation($path, 'not supported!');
    }

    /*
     * BunnyCDN doesn't give mimeType, so we need to get it on our own
     */
    public function mimeType(string $path): FileAttributes
    {
        if (!$this->fileExists($path)) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }

        $detector = new ExtensionMimeTypeDetector();
        $mimeType = $detector->detectMimeTypeFromFile($path);

        if ($mimeType === null || $mimeType === '') {
            $detector = new FinfoMimeTypeDetector();
            $mimeType = $detector->detectMimeType($path, $this->read($path));
        }

        if ($mimeType === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }

        return new FileAttributes(
            $path,
            null,
            null,
            null,
            $mimeType
        );
    }

    public function checksum(string $path, Config $config): string
    {
        return $this->calculateChecksumFromStream($path, $config);
    }

    /*
     * we need to catch here while S3 results in Exception when deleting a not existing resource
     */
    public function delete($path): void
    {
        try {
            parent::delete($path);
        } catch (UnableToDeleteFile) {
        }
    }

    public function deleteDirectory($path): void
    {
        $this->delete(rtrim($path, '/') . '/');
    }

    public function directoryExists($path): bool
    {
        $pathParts = explode('/', rtrim($path, '/'));

        $path = $pathParts[array_key_last($pathParts)];
        unset($pathParts[array_key_last($pathParts)]);

        $directoryContent = iterator_to_array($this->listContents(implode('/', $pathParts), false));
        $directoryContent = json_decode(json_encode($directoryContent));

        return \count(array_filter($directoryContent, function ($a) use ($path) {
            return $a->type === 'dir' && $a->path === $path;
        })) > 0;
    }
}
