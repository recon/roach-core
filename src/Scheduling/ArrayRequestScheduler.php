<?php

declare(strict_types=1);

/**
 * Copyright (c) 2021 Kai Sassnowski
 *
 * For the full copyright and license information, please view
 * the LICENSE file that was distributed with this source code.
 *
 * @see https://github.com/roach-php/roach
 */

namespace Sassnowski\Roach\Scheduling;

use DateInterval;
use DateTimeImmutable;
use Sassnowski\Roach\Http\Request;
use Sassnowski\Roach\Scheduling\Timing\ClockInterface;

final class ArrayRequestScheduler implements RequestSchedulerInterface
{
    private int $batchSize = 25;

    private int $delay = 0;

    private array $requests = [];

    private DateTimeImmutable $nextBatchReadyAt;

    public function __construct(private ClockInterface $clock)
    {
        $this->nextBatchReadyAt = $this->clock->now();
    }

    public function schedule(Request $request): void
    {
        $this->requests[] = $request;
    }

    public function empty(): bool
    {
        return empty($this->requests);
    }

    public function nextRequests(): array
    {
        $this->clock->sleepUntil($this->nextBatchReadyAt);

        $this->updateNextBatchTime();

        return \array_splice($this->requests, 0, $this->batchSize);
    }

    public function setBatchSize(int $batchSize): RequestSchedulerInterface
    {
        $this->batchSize = $batchSize;

        return $this;
    }

    public function setDelay(int $delay): RequestSchedulerInterface
    {
        $this->delay = $delay;

        return $this;
    }

    private function updateNextBatchTime(): void
    {
        $this->nextBatchReadyAt = $this->clock->now()->add(new DateInterval("PT{$this->delay}S"));
    }
}