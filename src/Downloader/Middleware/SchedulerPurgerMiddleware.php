<?php

namespace RoachPHP\Downloader\Middleware;

use RoachPHP\Downloader\DownloaderMiddlewareInterface;
use RoachPHP\Http\Request;
use RoachPHP\Http\Response;
use RoachPHP\ItemPipeline\ItemInterface;
use RoachPHP\Scheduling\RequestSchedulerInterface;
use RoachPHP\Spider\SpiderMiddlewareInterface;
use RoachPHP\Support\Configurable;

/**
 * When invoked, will reset any persisted scheduler queue.
 * Only makes sense with persisted scheduler, ie DatabaseRequestScheduler
 */
class SchedulerPurgerMiddleware implements DownloaderMiddlewareInterface
{
    use Configurable;


    public function __construct(
        RequestSchedulerInterface $requestScheduler,
    )
    {

        $requestScheduler->purge();
    }


    public function handleResponse(Response $response): Response
    {
        return $response;
    }

    public function handleRequest(Request $request): Request
    {
        return $request;
    }
}