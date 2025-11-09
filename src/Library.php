<?php

namespace SeaDuck;

class Library
{
    public static function check($event = null)
    {
        return \Saturio\DuckDB\CLib\Installer::install($event);
    }
}
