<?php
namespace Splitice\DB;

use Radical\Database\Model\Table;

/**
 * @package Splitice\DB
 */
class JobWorker extends Table
{
	const TABLE_PREFIX = 'jw_';
	const TABLE = 'job_worker';

	protected $id;
	protected $started;
}