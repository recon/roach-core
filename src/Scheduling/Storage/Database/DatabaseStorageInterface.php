<?php

namespace RoachPHP\Scheduling\Storage\Database;

use RoachPHP\Http\Request;

interface DatabaseStorageInterface
{
    public function setNamespace(string $namespace);

    public function pushItem(Request $request, string $path);

    public function pullItems(int $batchSize): array;

    public function isEmpty(): bool;

    public function purge(): void;
}
